
import json
import plistlib
import re

from contextlib import suppress
from datetime import datetime, timedelta
from functools import cached_property
from itertools import chain
from pathlib import Path
from queue import Queue
from subprocess import CompletedProcess, run, Popen, PIPE, CalledProcessError, TimeoutExpired
from tempfile import TemporaryDirectory
from time import sleep

from . import CONFIG_DIR, CACHE_DIR
from .filesystem import Directory, Root
from .util import format_size


# - continue with backup while waiting for user decision? (skip large new dir for now)
#   no, wait for user input
# - show icon in menu bar, with dropdown menu
#   - list of oversized dirs/files with their size; clicking copies path to clipboard
#   - continue: go ahead and backup new large items
#   - show diff: open terminal with ncdu of changes to be backed up
#   - open exclude file
#   - ? skip backup: try backup again in x hours
# - allow-file to record paths of allowed large files/dirs
#   (what about new large files below this dir?)
#     -> should have option to allow once
# - record snapshot name in case backup is aborted and resume later?
#   (does it really matter?)
# - keep ncdu along with log file

# change ~/Library to include-only


# phases:
# 0) check whether current snapshot is already backed up
# 1) prepare: determine size of backups and check threshold
# 2) wait for user feedback
# 3) backup

RE_SNAPSHOT = re.compile(r'com\.apple\.TimeMachine\.(\d{4}-\d{2}-\d{2}-\d{6})\.local')

def snapshot_datetime(snapshot_name):
    timestamp = RE_SNAPSHOT.match(snapshot_name).group(1)
    return datetime.fromisoformat(timestamp)


DF = '/bin/df'
DISKUTIL = '/usr/sbin/diskutil'
TMUTIL = '/usr/bin/tmutil'
MOUNT_APFS = '/sbin/mount_apfs'
UMOUNT = '/sbin/umount'

# Local snapshots can be purged by macOS at any time. If that happens while a
# backup is running, retry with a fresh snapshot a few times before giving up.
MAX_SNAPSHOT_RETRIES = 2


class RcloneMixin:

    def _run(self, args, echo=False, dry_run=False, **kwargs):
        if echo:
            _print_log(' '.join(map(str, args)))
        if not dry_run:
            return run(args, **kwargs)

    def rclone(self, subcmd, *args, dry_run=None, capture=False) \
            -> CompletedProcess or None:
        """Run short-running rclone command with the given arguments

        Args:
          args: command line arguments passed to rclone
          dry_run: if not None, overrides dry_run set for the instance
          capture: capture the output (in stdout attribute of return value)

        Returns:
          rclone CompletedProcess object
        """
        dry_run = self.dry_run if dry_run is None else dry_run
        cmd = [self.rclone_path, subcmd, '--use-json-log', *args]
        stdout = PIPE if capture else None
        while True:
            try:
                process = self._run(cmd, echo=self.echo or dry_run, dry_run=dry_run,
                                    stdout=stdout, stderr=PIPE, encoding='utf-8',
                                    check=True)
                return process
            except CalledProcessError as cpe:
                for line in cpe.stderr.splitlines():
                    if not (log_entry := try_json(line)):
                        print(line)
                        continue
                    msg = log_entry['msg']
                    if ("no such host" in msg
                            or "network is unreachable" in msg
                            or "error listing: couldn't list files" in msg):
                        print("Connection error; retrying in 5 seconds...")
                        sleep(5)
                        break
                    raise RcloneError(cpe.returncode, log_entry)

    def list_files(self, *include, exclude=None, recursive=True,
                   dirs_only=False, files_only=False):
        args = ((['--dirs-only'] if dirs_only else [])
                + (['--files-only'] if files_only else [])
                + [*chain.from_iterable(['--include', inc] for inc in include)]
                + (['--exclude', exclude] if exclude else [])
                + (['--recursive'] if recursive else []))
        list_cmd = self.rclone('lsf', self.destination, '--dir-slash=false',
                               *args, dry_run=False, capture=True)
        return sorted(list_cmd.stdout.splitlines())


class BackupConfig(RcloneMixin):

    def __init__(self, name, source, destination, interval, threshold,
                 bwlimit=None, rclone='rclone', echo=False, progress=False,
                 dry_run=False):
        self.name = name
        self.source = Path(source)
        self.destination = Path(destination)
        self.interval = interval
        self.threshold = threshold
        self.bwlimit = bwlimit
        self.rclone_path = rclone
        self.echo = echo
        self.progress = progress
        self.dry_run = dry_run

    @cached_property
    def source_device_volume(self):
        df = self._run([DF, self.source], encoding='utf-8', check=True,
                       capture_output=True, echo=self.echo).stdout
        _, data = df.splitlines()
        device, *_, mounted_on = data.split(maxsplit=8)
        if mounted_on == '/':
            raise RuntimeError("Cannot back up the root volume")
        return device, mounted_on

    @property
    def exclude_file(self):
        return CONFIG_DIR / f'{self.name}.exclude'

    def get_last_snapshot(self):
        device, _ = self.source_device_volume
        while True:
            output = self._run([DISKUTIL, 'apfs', 'listSnapshots', '-plist', device],
                               echo=self.echo, check=True, capture_output=True).stdout
            with suppress(IndexError):
                snapshot = plistlib.loads(output)['Snapshots'][-1]['SnapshotName']
                if datetime.now() - snapshot_datetime(snapshot) < timedelta(hours=1):
                    break
            self._run([TMUTIL, 'localsnapshot'], echo=self.echo, check=True)
        return device, snapshot

    def get_last_log(self):
        try:
            logs = self.list_files(f"/{self.name}_*_sync.log", recursive=False,
                                files_only=True)
        except RcloneError as exc:
            if exc.log_entry['msg'] == "error listing: directory not found":
                return None     # this is the first backup
            raise
        try:
            last_log, = logs
        except ValueError:
            raise SystemExit(f"There should only be a single sync log file in"
                             f" {self.destination}!")
        return last_log

    def last_backups(self, number=10):
        try:
            snapshots = self.list_files(recursive=False, dirs_only=True)
        except RcloneError as exc:
            if exc.log_entry['msg'] == "error listing: directory not found":
                return
            raise
        dirs_list = ','.join(snapshots[-number:])
        last_size_file = f'/{self.name}_*_transferred_*'
        size_files = f"/{{{dirs_list}}}/{self.name}_*_transferred_*"
        sizes = self.list_files(last_size_file, size_files, recursive=True,
                                files_only=True)
        for size_path in reversed(sizes):
            prefix, snapshot, _, size = size_path.rsplit('_', maxsplit=3)
            sync_ncdu_json = self.destination / f'{prefix}_{snapshot}_sync.json'
            yield snapshot, int(size), sync_ncdu_json

    def backup(self, app, force=False):
        app.starting(self)
        if not (self.interval or force):    # backups without interval set need
            return False                    #  to be started manually
        for attempt in range(MAX_SNAPSHOT_RETRIES + 1):
            try:
                device, snapshot = self.get_last_snapshot()
            except VolumeNotMounted as exc:
                app.notify_volume_not_mounted(self, exc.volume)
                return False
            last_log = self.get_last_log()
            local_timestamp = snapshot_datetime(snapshot)
            if last_log:
                log_timestamp = timestamp_from_log(last_log)
                last_age = local_timestamp - datetime.fromisoformat(log_timestamp)
                if last_age == timedelta(0):
                    print(f"{self.name}: the last local snapshot was already backed up")
                    if force:
                        app.last_snapshot_already_backed_up(self, local_timestamp)
                    return False
                elif not force and last_age < self.interval:
                    print(f"{self.name}: last backup is only {last_age} old (< {self.interval})")
                    return False
            try:
                task = BackupTask(self, device, snapshot, last_log, app)
            except TimeMachineBackupInProgress:
                return False
            except NoFullDiskAccess:
                app.notify_no_full_disk_access(self)
                return False
            try:
                task.perform()
            except SnapshotRemoved as exc:
                _print_log(f"{self.name}: {exc}")
                if attempt == MAX_SNAPSHOT_RETRIES:
                    _print_log(f"{self.name}: giving up after"
                               f" {attempt + 1} attempts")
                    return False
                # make sure the retry uses a snapshot that is newer than the
                # last successful backup, even if the purged snapshot was the
                # only one that was recent enough
                self._run([TMUTIL, 'localsnapshot'], echo=self.echo, check=True)
                _print_log(f"{self.name}: retrying with a fresh snapshot")
                continue
            return True
        return False

    RE_SIZE = re.compile(r"^(.+)\/.*_\1_size_(\d+)$")

    def print_snapshot_sizes(self):
        size_files = self.list_files(f"/*/{self.name}_*_size_*",
                                     files_only=True)
        total = 0
        print(f"Size of snapshots in {self.destination}")
        for path in size_files:
            timestamp, size_str = self.RE_SIZE.match(path).groups()
            size = int(size_str)
            print(f"{timestamp:18} {format_size(size, True):>12}")
            total += size
        print(f"{'Total:':18} {format_size(total, True):>12}")


class BackupTask(RcloneMixin):

    def __init__(self, config, device, snapshot, last_log, app):
        self.config = config
        self._app = app
        self.last_log = last_log
        self._tempdir = TemporaryDirectory()
        self.snapshot = snapshot
        self.mount_point = self.mount_snapshot(device, snapshot)
        self.timestamp = RE_SNAPSHOT.match(snapshot).group(1)
        self.exclude_queue = Queue(maxsize=1)
        self.expected_size = 0

    def __getattr__(self, name):
        # if attribute isn't set in this class, look it up in the configuration
        return getattr(self.config, name)

    @property
    def destination_latest(self):
        return self.destination / 'latest'

    @property
    def backup_directory(self):
        if self.last_log:
            return self.destination / timestamp_from_log(self.last_log)
        return None

    @property
    def logs_path(self):
        return CACHE_DIR / self.name

    def file_path(self, label, extension):
        basename = self.logs_path / f'{self.name}_{self.timestamp}_{label}'
        return basename.with_suffix(f'.{extension}')

    @property
    def scout_ncdu_export_path(self):
        return self.file_path('scout', 'json')

    @property
    def sync_ncdu_export_path(self):
        return self.file_path('sync', 'json')

    @property
    def large_files_path(self):
        return self.file_path('large', 'log')

    def perform(self):
        self.logs_path.mkdir(parents=True, exist_ok=True)
        try:
            self.verify_snapshot()
            self.rclone('mkdir', self.destination_latest, dry_run=False)
            self._app.sizing(self)
            tree = self.backup_scout()
            self.verify_snapshot()
            backup_size, exclude = self.get_user_feedback(tree)
            if backup_size is None:     # user skipped the backup
                self._app.wrapping_up(self)
            elif backup_size != 0:
                self.verify_snapshot()
                self.expected_size = backup_size
                self._app.start_backup(self, backup_size)
                self.backup_sync(tree, exclude)
                self._app.wrapping_up(self)
                self.finalize()
        finally:
            _end_progress()
            self.cleanup()

    def verify_snapshot(self):
        """Make sure the snapshot is still mounted.

        Local snapshots are purgeable and may be deleted by macOS while they
        are being backed up, which also unmounts them. Detect this so that we
        do not treat a partial (or empty) backup as complete.
        """
        if not self.mount_point.is_mount():
            raise SnapshotRemoved(
                f"the local snapshot {self.snapshot} was removed while mounted")

    def cleanup(self):
        self.unmount_snapshot()
        del self._tempdir

    def mount_snapshot(self, device, snapshot):
        mount_point = Path(self._tempdir.name)
        print(f'Mounting {snapshot} at {mount_point}')
        try:
            self._run([MOUNT_APFS, '-s', snapshot, '-o', 'nobrowse', device,
                       mount_point], echo=self.echo, check=True)
        except CalledProcessError as cpe:
            if cpe.returncode == 75:
                raise TimeMachineBackupInProgress
            if cpe.returncode == 77:    # permission denied
                raise NoFullDiskAccess
            raise
        return mount_point

    def unmount_snapshot(self):
        try:
            run([UMOUNT, self.mount_point], check=True)
        except CalledProcessError:
            # Local snapshots are purgeable and may be deleted by macOS while
            # they are mounted (e.g. when Time Machine makes a new snapshot or
            # disk space runs low). Deleting a snapshot also unmounts it, in
            # which case there is nothing left to unmount.
            if self.mount_point.is_mount():
                raise
            _print_log(f'{self.mount_point} was unmounted'
                       ' (the snapshot was removed)')

    def get_user_feedback(self, tree):
        backup_size = tree.transfer_size
        large_entries = (sorted(tree.large_entries(self.threshold),
                                key=lambda e: e.transfer_size, reverse=True)
                         if self.threshold else [])
        if large_entries:
            with self.large_files_path.open('w') as f:
                for entry in large_entries:
                    size = format_size(entry.transfer_size, True)
                    suffix = '/' if isinstance(entry, Directory) else ''
                    print(f'{size}   {entry.path}{suffix}', file=f)
            self._app.threshold_exceeded(self, backup_size, large_entries)
            exclude = self.exclude_queue.get()
            if exclude is None:     # user skipped the backup
                backup_size = None
            else:
                backup_size -= sum(entry.transfer_size for entry in exclude)
        else:
            exclude = []
        return backup_size, exclude

    def sync_popen(self, *args, dry_run=False):
        _, volume = self.source_device_volume
        source_root = '/' if volume == '/System/Volumes/Data' else volume
        snapshot_source = self.mount_point / self.source.relative_to(source_root)
        extra = list(chain(['--bwlimit', self.bwlimit] if self.bwlimit else [],
                           ['--dry-run'] if dry_run else [],
                           ['--stats', '1s'] if self.progress else []))
        # --stats 1s generates periodic JSON stats messages on stderr
        # (via --use-json-log) which are parsed in sync_log_to_tree to show
        # a single-line progress indicator when progress=True.
        cmd = [self.rclone_path, 'sync', '--use-json-log', '--log-level', 'INFO',
               '--fast-list', '--links', '--track-renames',
               '--track-renames-strategy', 'modtime,leaf', *args, *extra,
               snapshot_source, self.destination_latest]
        if self.echo:
            _print_log(' '.join(map(str, cmd)))
        return Popen(cmd, stderr=PIPE)

    @staticmethod
    def _wait(process, timeout=3):
        """Wait for an rclone process whose output was read to the end.

        Returns the exit status. rclone sometimes does not exit when its log
        output is complete; terminate it in that case.
        """
        try:
            return process.wait(timeout=timeout)
        except TimeoutExpired:
            with suppress(ProcessLookupError):
                process.terminate()
            process.wait()
            return 0

    def backup_scout(self):
        args = ['--retries', '1']
        if self.exclude_file.exists():
            args.extend(['--exclude-from', self.exclude_file])
        scout = self.sync_popen(*args, dry_run=True)
        try:
            scout_log = self.file_path('scout', 'log')
            with scout_log.open('wb') as log:
                tree = scout_log_to_tree(self.source, scout.stderr, log)
        except CalledProcessError:
            self.verify_snapshot()
            raise
        returncode = self._wait(scout)
        self.verify_snapshot()
        if returncode:
            raise CalledProcessError(returncode, scout.args)
        tree.write_ncdu_export(self.scout_ncdu_export_path)
        return tree

        # TODO: caffeinate
        # FIXME: abort subprocesses on App quit
    def backup_sync(self, tree, exclude):
        files_txt = self.file_path('files', 'txt')
        with files_txt.open('w') as files:
            for file in tree.iter_files(exclude=exclude):
                print(file.path, file=files)
                if file.path.suffix == '.rclonelink':   # rclone issue #6855
                    print(file.path.with_suffix(''), file=files)
        backupdir_args = (['--backup-dir', self.backup_directory]
                          if self.backup_directory else [])
        try:
            sync = self.sync_popen('--files-from-raw', files_txt,
                                   *backupdir_args, dry_run=self.dry_run)
            transferred = 0
            moved_bytes = 0

            def update_progress():
                # rclone tracks server-side moves (e.g. overwritten files moved
                # to the backup directory) separately from transferred bytes, so
                # count them as work that has already been done
                total = self.expected_size + moved_bytes
                self._app.update_progress(self, transferred + moved_bytes, total)

            def on_stats(stats):
                nonlocal moved_bytes
                moved_bytes = stats.get('serverSideMoveBytes') or 0
                update_progress()

            sync_log = self.file_path('sync', 'log')
            with sync_log.open('wb') as log:
                it = sync_log_to_tree(tree, sync.stderr, log, self.dry_run,
                                      on_stats=on_stats)
                sync_tree = next(it)
                for item in it:
                    try:
                        transferred += item.size
                    except TypeError as e:
                        _print_log(e)
                        _print_log(item.path)
                    update_progress()
        except CalledProcessError as cpe:
            self.verify_snapshot()
            rc = cpe.returncode
            info = RCLONE_EXIT_CODES.get(rc, "Unknown error")
            _print_log(f"rclone returned non-zero exit status {rc} - {info}")
            _print_log(f"The log file is {sync_log}")
            raise
        returncode = self._wait(sync)
        self.verify_snapshot()
        if returncode:
            raise CalledProcessError(returncode, sync.args)
        _end_progress()
        sync_tree.write_ncdu_export(self.sync_ncdu_export_path)
        transferred_filename = f'{self.name}_{self.timestamp}_transferred_{transferred}'
        self.rclone('touch', self.destination / transferred_filename)
        return True

    def continue_backup(self, excluded):
        self.exclude_queue.put_nowait(excluded)

    def skip_backup(self):
        self.exclude_queue.put_nowait(None)

    def finalize(self):
        if self.backup_directory:
            # move the logs from the last backup to the backup dir
            last_logs = '/' + self.last_log.replace('sync.log', '*')
            self.rclone('move', '--include', last_logs,
                        self.destination, self.backup_directory)
            self.record_backup_size(self.backup_directory)
        # copy logs for this backup to the remote
        local_logs = self.file_path('*', '*')
        self.rclone('copy', '--include', local_logs.name,
                    local_logs.parent, self.destination)

    def record_backup_size(self, backupdir):
        size_cmd = self.rclone('size', '--json', backupdir, capture=True)
        size = 0 if self.dry_run else json.loads(size_cmd.stdout)['bytes']
        size_filename = f'{self.name}_{backupdir.name}_size_{size}'
        self.rclone('touch', backupdir / size_filename)


_prev_progress_lines = 0


def _end_progress():
    """Erase the progress indicator drawn by _show_progress(), if any."""
    global _prev_progress_lines
    if _prev_progress_lines > 0:
        print(f'\033[{_prev_progress_lines}A\033[J', end='', flush=True)
        _prev_progress_lines = 0


def _print_log(*args, **kwargs):
    """Print output that should not be overwritten by the progress indicator."""
    _end_progress()
    print(*args, **kwargs)


def _show_progress(msg_text):
    """Display a multi-line progress indicator, overwriting the previous one."""
    global _prev_progress_lines

    # Move cursor up and clear previously printed lines
    _end_progress()

    # Print the new message
    print(msg_text, end='', flush=True)

    # Count lines just printed (msg_text may or may not end with newline)
    _prev_progress_lines = msg_text.count('\n')
    if not msg_text.endswith('\n'):
        _prev_progress_lines += 1


RE_RENAMED_FROM = re.compile('Renamed from "(.*)"')


def scout_log_to_tree(root_path, lines, log_file=None):
    tree = Root(root_path)
    for line in lines:
        if log_file:
            log_file.write(line)
        if not (msg := try_json(line)):
            _print_log(line)
            continue
        if msg.get('stats') is not None:
            _show_progress(msg['msg'])
            continue  # don't write ephemeral stats messages to log
        if skipped := msg.get('skipped'):
            if skipped == 'remove directory':
                break   # no need to handle explicitly?
            tree.add_file(msg['object'], msg['size'], action=skipped)
        elif m := RE_RENAMED_FROM.fullmatch(msg['msg']):
            source = m.group(1)
            tree.add_file(msg['object'], 0, action='move-dest', source=source)
            tree.get(source).metadata['destination'] = msg['object']
    return tree


def sync_log_to_tree(scout_tree, lines, log_file=None, dry_run=False,
                     on_stats=None):
    sync_tree = Root(scout_tree.source_path)
    yield sync_tree
    for line in lines:
        if log_file:
            log_file.write(line)
        if not (msg := try_json(line)):
            _print_log(line)
            continue
        if msg.get('stats') is not None:
            _show_progress(msg['msg'])
            if on_stats:
                on_stats(msg['stats'])
            continue  # don't write ephemeral stats messages to log
        if (msg['msg'].startswith('Copied')
                or (dry_run and msg.get('skipped') == 'copy')):
            file_path = msg['object']
            item = scout_tree.get(file_path)
            sync_tree.add_file(file_path, item.size, action='copy')
            yield item
        elif msg['level'] == 'error':
            _print_log('ERROR:', msg['msg'])


def try_json(line):
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None


def timestamp_from_log(log_filename):
    _, timestamp, _ = log_filename.rsplit('_', maxsplit=2)
    return timestamp


# https://rclone.org/docs/#exit-code
RCLONE_EXIT_CODES = {
  1: "Syntax or usage error",
  2: "Error not otherwise categorised",
  3: "Directory not found",
  4: "File not found",
  5: "Temporary error (one that more retries might fix) (Retry errors)",
  6: "Less serious errors (like 461 errors from dropbox) (NoRetry errors)",
  7: "Fatal error (one that more retries won't fix, like account suspended) (Fatal errors)",
  8: "Transfer exceeded - limit set by --max-transfer reached",
  9: "Operation successful, but no files transferred",
}


class RcloneError(Exception):
    """Error reported by rclone"""
    def __init__(self, returncode, log_entry):
        super().__init__()
        self.returncode = returncode
        self.log_entry = log_entry


class VolumeNotMounted(Exception):
    """Volume referenced in configuration is not mounted"""

    def __init__(self, volume):
        super().__init__()
        self.volume = volume


class TimeMachineBackupInProgress(Exception):
    pass


class NoFullDiskAccess(Exception):
    pass


class SnapshotRemoved(Exception):
    """The mounted local snapshot was removed (e.g. purged by macOS)"""
