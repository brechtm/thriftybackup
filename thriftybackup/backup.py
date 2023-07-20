
import json
import plistlib
import re

from datetime import datetime, timedelta
from itertools import chain
from pathlib import Path
from queue import Queue
from subprocess import CompletedProcess, run, Popen, PIPE, CalledProcessError
from tempfile import TemporaryDirectory

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

RE_SNAPSHOT = re.compile('com\.apple\.TimeMachine\.(\d{4}-\d{2}-\d{2}-\d{6})\.local')

def snapshot_datetime(snapshot_name):
    timestamp = RE_SNAPSHOT.match(snapshot_name).group(1)
    return datetime.fromisoformat(timestamp)


DISKUTIL = '/usr/sbin/diskutil'
TMUTIL = '/usr/bin/tmutil'
MOUNT_APFS = '/sbin/mount_apfs'
UMOUNT = '/sbin/umount'


class RCloneBackup:

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

        self._app = None
        self.tree = None
        self.exclude_queue = Queue(maxsize=1)
        self._tempdir = None
        self.mount_point = None

    @property
    def source_volume(self):
        if self.source.is_relative_to('/Volumes'):
            return Path(*self.source.parts[:3])
        else:
            return None

    @property
    def destination_latest(self):
        return self.destination / 'latest'

    @property
    def exclude_file(self):
        return CONFIG_DIR / f'{self.name}.exclude'        

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

    def _run(self, args, echo=False, dry_run=False, **kwargs):
        if echo:
            print(' '.join(map(str, args)))
        if not dry_run:
            return run(args, **kwargs)

    def cleanup(self):
        # TODO: stop thread
        if hasattr(self, '_tempdir'):
            self.unmount_snapshot()
            del self._tempdir

    def get_last_snapshot(self):
        source_mount = self.source_volume or '/System/Volumes/Data'
        try:
            du_info = self._run([DISKUTIL, 'info', '-plist', source_mount],
                                echo=self.echo, check=True, capture_output=True).stdout
        except CalledProcessError as exc:
            if exc.returncode == 1:
                raise VolumeNotMounted(source_mount)
            raise
        device = plistlib.loads(du_info)['DeviceIdentifier']
        while True:
            output = self._run([DISKUTIL, 'apfs', 'listSnapshots', '-plist', device],
                               echo=self.echo, check=True, capture_output=True).stdout
            snapshot = plistlib.loads(output)['Snapshots'][-1]['SnapshotName']
            if datetime.now() - snapshot_datetime(snapshot) < timedelta(hours=1):
                break
            self._run([TMUTIL, 'localsnapshot'], echo=self.echo, check=True)
        return device, snapshot

    def mount_snapshot(self, device, snapshot):
        self._tempdir = TemporaryDirectory()
        self.mount_point = Path(self._tempdir.name)
        print(f'Mounting {snapshot} at {self.mount_point}')
        try:
            run([MOUNT_APFS, '-s', snapshot, '-o', 'nobrowse',
                 f'/dev/{device}', self.mount_point], check=True)
        except CalledProcessError as cpe:
            if cpe.returncode == 75:
                raise TimeMachineBackupInProgress
            raise
        
    def unmount_snapshot(self):
        run([UMOUNT, self.mount_point], check=True)        

    def backup(self, app, force=False):
        if not (self.interval or force):    # backups without interval set need
            return False                    #  to be started manually
        try:
            device, snapshot = self.get_last_snapshot()
        except VolumeNotMounted as exc:
            app.notify_volume_not_mounted(self, exc.volume)
            return False
        self.timestamp = RE_SNAPSHOT.match(snapshot).group(1)
        self.logs_path.mkdir(parents=True, exist_ok=True)
        try:
            self.rclone('mkdir', self.destination_latest, dry_run=False)
            last_log = self.get_last_log()
        except CalledProcessError as error:
            if error.returncode == 1:   # connection error
                return False
            raise
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
            self.mount_snapshot(device, snapshot)
        except TimeMachineBackupInProgress:
            return False
        self._app = app
        try:
            return self.perform_backup(last_log)
        finally:
            self.cleanup()

    def rclone(self, *args, dry_run=None, capture=False) -> CompletedProcess or None:
        """Run short-running rclone command with the given arguments
        
        Args:
          args: command line arguments passed to rclone
          dry_run: if not None, overrides dry_run set for the instance
          capture: capture the output (in stdout attribute of return value)
        
        Returns:
          rclone CompletedProcess object
        """
        dry_run = self.dry_run if dry_run is None else dry_run
        cmd = [self.rclone_path, *args]
        return self._run(cmd, echo=self.echo or dry_run, dry_run=dry_run,
                         capture_output=capture, encoding='utf-8', check=True)

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

    def last_backups(self, number=10):
        snapshots = self.list_files(recursive=False, dirs_only=True)
        assert snapshots.pop() == 'latest'
        dirs_list = ','.join(snapshots[-number:])
        last_size_file = f'/{self.name}_*_transferred_*'
        size_files = f"/{{{dirs_list}}}/{self.name}_*_transferred_*"
        sizes = self.list_files(last_size_file, size_files, recursive=True,
                                files_only=True)
        for size_path in reversed(sizes):
            prefix, snapshot, _, size = size_path.rsplit('_', maxsplit=3)
            sync_ncdu_json = self.destination / f'{prefix}_{snapshot}_sync.json'
            yield snapshot, int(size), sync_ncdu_json

    def get_last_log(self):
        logs = self.list_files(f"/{self.name}_*_sync.log", recursive=False,
                               files_only=True)
        if not logs:  # this is the first backup
            return None
        try:
            last_log, = logs
        except ValueError:
            raise SystemExit(f"There should only be a single sync log file in"
                             f" {self.destination}!")
        return last_log

    def perform_backup(self, last_log):
        self._app.prepare(self)
        self.tree = self.backup_scout()
        backup_size = self.tree.transfer_size
        backup_performed = False
        large_entries = sorted(self.tree.large_entries(self.threshold),
                               key=lambda e: e.transfer_size, reverse=True)
        if large_entries:
            with self.large_files_path.open('w') as f:
                for entry in large_entries:
                    size = format_size(entry.transfer_size, True)
                    suffix = '/' if isinstance(entry, Directory) else ''
                    print(f'{size}   {entry.path}{suffix}', file=f)
            self._app.threshold_exceeded(self, backup_size, large_entries)
            exclude = self.exclude_queue.get()
            if exclude is None:     # user skipped the backup
                backup_size = 0
            else:
                backup_size -= sum(entry.transfer_size for entry in exclude)
        else:
            exclude = []
        if backup_size != 0:
            self._app.start_backup(self, backup_size)
            backup_dir = (self.destination / timestamp_from_log(last_log)
                        if last_log else None)
            success = self.backup_sync(backup_dir, exclude)
            self._app.finish_backup(self)
            if backup_dir:
                # move the logs from the last backup to the backup dir
                last_logs = '/' + last_log.replace('sync.log', '*')
                self.rclone('move', '--include', last_logs,
                            self.destination, backup_dir)
                self.record_backup_size(backup_dir)
            # copy logs for this backup to the remote
            local_logs = self.file_path('*', '*')
            self.rclone('copy', '--include', local_logs.name,
                        local_logs.parent, self.destination)
            backup_performed = True
        self._app.idle()
        return backup_performed

    def sync_popen(self, *args, dry_run=False):
        source_root = self.source_volume or '/'
        snapshot_source = self.mount_point / self.source.relative_to(source_root)
        extra = list(chain(['--bwlimit', self.bwlimit] if self.bwlimit else [],
                           ['--dry-run'] if dry_run else [],
                           ['--progress'] if self.progress else []))
        cmd = [self.rclone_path, 'sync', '--use-json-log', '--log-level', 'INFO',
               '--fast-list', '--links', '--track-renames',
               '--track-renames-strategy', 'modtime,leaf', *args, *extra,
               snapshot_source, self.destination_latest]
        if self.echo:
            print(' '.join(map(str, cmd)))
        return Popen(cmd, stderr=PIPE)

    def backup_scout(self):
        args = ['--retries', '1']
        if self.exclude_file.exists():
            args.extend(['--exclude-from', self.exclude_file])
        try:
            rclone_sync = self.sync_popen(*args, dry_run=True)
            scout_log = self.file_path('scout', 'log')
            with scout_log.open('wb') as log:
                tree = scout_log_to_tree(self.source, rclone_sync.stderr, log)
        except CalledProcessError as cpe:
            # TODO: interpret rclone_sync.returncode
            raise
        tree.write_ncdu_export(self.scout_ncdu_export_path)
        return tree

        # TODO: caffeinate
        # FIXME: abort subprocesses on App quit
    def backup_sync(self, backup_dir, exclude):
        files_txt = self.file_path('files', 'txt')
        with files_txt.open('w') as files:
            for file in self.tree.iter_files(exclude=exclude):
                print(file.path, file=files)
                if file.path.suffix == '.rclonelink':   # rclone issue #6855
                    print(file.path.with_suffix(''), file=files)
        backupdir_args = ['--backup-dir', backup_dir] if backup_dir else []
        try:
            sync = self.sync_popen('--files-from-raw', files_txt,
                                   *backupdir_args, dry_run=self.dry_run)
            transferred = 0
            sync_log = self.file_path('sync', 'log')
            with sync_log.open('wb') as log:
                it = sync_log_to_tree(self.tree, sync.stderr, log, self.dry_run)
                sync_tree = next(it)
                for item in it:
                    transferred += item.size
                    self._app.update_progress(self, transferred)
            sync_tree.write_ncdu_export(self.sync_ncdu_export_path)
        except CalledProcessError as cpe:
            rc = cpe.returncode
            info = RCLONE_EXIT_CODES[rc]
            print(f"rclone returned non-zero exit status {rc} - {info}")
            print(f"The log file is {sync_log}")
            return False
        transferred_filename = f'{self.name}_{self.timestamp}_transferred_{transferred}'
        self.rclone('touch', self.destination / transferred_filename)
        return True

    def continue_backup(self, excluded):
        self.exclude_queue.put_nowait(excluded)
        
    def skip_backup(self):
        self.exclude_queue.put_nowait(None)

    def record_backup_size(self, backupdir):
        size_cmd = self.rclone('size', '--json', backupdir, capture=True)
        size = 0 if self.dry_run else json.loads(size_cmd.stdout)['bytes']
        size_filename = f'{self.name}_{backupdir.name}_size_{size}'
        self.rclone('touch', backupdir / size_filename)

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


RE_RENAMED_FROM = re.compile('Renamed from "(.*)"')


def scout_log_to_tree(root_path, lines, log_file=None):
    tree = Root(root_path)
    for line in lines:
        if log_file:
            log_file.write(line)
        if not (msg := try_json(line)):
            print(line)
            continue
        if skipped := msg.get('skipped'):
            if skipped == 'remove directory':
                break   # no need to handle explicitly?
            tree.add_file(msg['object'], msg['size'], action=skipped)
        elif m := RE_RENAMED_FROM.fullmatch(msg['msg']):
            source = m.group(1)
            tree.add_file(msg['object'], 0, action='move-dest', source=source)
            tree.get(source).metadata['destination'] = msg['object']
    return tree


def sync_log_to_tree(scout_tree, lines, log_file=None, dry_run=False):
    sync_tree = Root(scout_tree.source_path)
    yield sync_tree
    for line in lines:
        if log_file:
            log_file.write(line)
        if not (msg := try_json(line)):
            print(line)
            continue
        if (msg['msg'].startswith('Copied')
                or (dry_run and msg.get('skipped') == 'copy')):
            file_path = msg['object']
            item = scout_tree.get(file_path)
            sync_tree.add_file(file_path, item.size, action='copy')
            yield item
        elif msg['level'] == 'error':
            print('ERROR:', msg['msg'])


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


class VolumeNotMounted(Exception):
    """Volume refernced in configuration is not mounted"""
    
    def __init__(self, volume):
        super().__init__()
        self.volume = volume


class TimeMachineBackupInProgress(Exception):
    pass
