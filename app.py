

import argparse
import json
import plistlib
import re
import sys
import time
import tomllib

from datetime import datetime, timedelta
from itertools import chain
from pathlib import Path
from subprocess import run, Popen, PIPE, DEVNULL, CalledProcessError
from tempfile import TemporaryDirectory
from threading import Thread

import pid
import rumps


PATH = Path(__file__).parent


class Entry:
    def __init__(self, path, size=None):
        self.path = path
        self.size = size

    def calculate_size(self):
        raise NotImplementedError

    def to_ncdu(self, name):
        raise NotImplementedError
        
        
class File(Entry):
    def calculate_size(self):
        return self.size

    def iter_files(self, exclude):
        if self not in exclude:
            yield self

    def to_ncdu(self, name):
        return dict(name=name, asize=self.size)


class Directory(Entry):
    def __init__(self, path):
        super().__init__(path)
        self.entries = {}

    def get(self, path: str) -> Entry:
        return self._get(path.split('/'))

    def _get(self, parts):
        name, *rest = parts
        return self.entries[name]._get(rest) if rest else self.entries[name]

    def add_file(self, path, size):
        parts = path.parts
        self._add_file(path, parts, size)

    def _add_file(self, path, path_parts, size):
        name, *parts = path_parts
        dir_path = '/'.join(path.parts[:-len(parts)]) + '/'
        if parts:
            dir = self.entries.setdefault(name, Directory(dir_path))
            dir._add_file(path, parts, size)
        else:
            assert name not in self.entries
            self.entries[name] = File(path, size)

    def calculate_size(self):
        self.size = sum((e.calculate_size() for e in self.entries.values()),
                        start=0)
        return self.size

    def iter_files(self, exclude):
        if self not in exclude:
            for entry in self.entries.values():
                yield from entry.iter_files(exclude)

    def to_ncdu(self, name):
        return [dict(name=name),
                *(entry.to_ncdu(name) for name, entry in self.entries.items())]


PREFIXES = {40: 'T', 30: 'G', 20: 'M', 10: 'K', 0: ' '}
EXPONENTS = {value: key for key, value in PREFIXES.items()}


def format_size(n_bytes, align=False):
    for exp, prefix in PREFIXES.items():
        if n_bytes > 2**exp:
            break
        if not align:
            prefix = prefix.strip()
    return f'{n_bytes / 2**exp:{8 if align else 0}.02f} {prefix}B'


def find_large_entries(entry, threshold):
    if entry.size < threshold:
        return
    try:
        entries = entry.entries
    except AttributeError:  # entry is a file
        yield entry
    else:                   # entry is a directory
        yield_this_dir = True
        for name, child in entries.items():
            for entry in find_large_entries(child, threshold):
                yield entry
                yield_this_dir = False
        if yield_this_dir:
            yield entry


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


def write_ncdu_export(root, tree, ncdu_export_path):
    ncdu = [1, 2, dict(progname=__file__, progver='0.0.0', timestamp=0),
            tree.to_ncdu(str(root))]
    with ncdu_export_path.open('w') as f:
        json.dump(ncdu, f)


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
                 bwlimit=None, rclone='rclone', echo=False, dry_run=False):
        self.name = name
        self.source = Path(source)
        self.destination = Path(destination)
        self.interval = interval
        self.threshold = threshold
        self.bwlimit = bwlimit
        self.rclone_path = rclone
        self.echo = echo
        self.dry_run = dry_run

        self.interface = None
        self.tree = None
        self.device, self.snapshot = self.get_last_snapshot()
        self.timestamp = RE_SNAPSHOT.match(self.snapshot).group(1)
        self._tempdir = None
        self.mount_point = None

    @property
    def destination_latest(self):
        return self.destination / 'latest'

    @property
    def exclude_file(self):
        return PATH / f'{self.name}.exclude'        

    @property
    def logs_path(self):
        return PATH / 'logs' / self.name

    def file_path(self, label, extension):
        basename = self.logs_path / f'{self.name}_{self.timestamp}_{label}'
        return basename.with_suffix(f'.{extension}')

    @property
    def ncdu_export_path(self):
        return self.file_path('ncdu', 'json')

    @property
    def large_files_path(self):
        return self.file_path('large', 'log')

    def cleanup(self):
        # TODO: stop thread
        if hasattr(self, '_tempdir'):
            self.unmount_snapshot()
            del self._tempdir

    def get_last_snapshot(self):
        du_info = run([DISKUTIL, 'info', '-plist', 'Data'],
                      check=True, capture_output=True).stdout
        device = plistlib.loads(du_info)['DeviceIdentifier']
        while True:
            output = run([DISKUTIL, 'apfs', 'listSnapshots', '-plist', device],
                         check=True, capture_output=True).stdout
            snapshot = plistlib.loads(output)['Snapshots'][-1]['SnapshotName']
            if datetime.now() - snapshot_datetime(snapshot) < timedelta(hours=1):
                break
            run([TMUTIL, 'localsnapshot'], check=True)
        return device, snapshot

    def mount_snapshot(self):
        self._tempdir = TemporaryDirectory()
        self.mount_point = Path(self._tempdir.name)
        print(f'Mounting {self.snapshot} at {self.mount_point}')
        run([MOUNT_APFS, '-s', self.snapshot,
             f'/dev/{self.device}', self.mount_point], check=True)
        
    def unmount_snapshot(self):
        run([UMOUNT, self.mount_point], check=True)        

    def backup(self, interface):
        self.logs_path.mkdir(parents=True, exist_ok=True)
        self.rclone('mkdir', self.destination_latest, dry_run=False)
        return self._backup(interface)

    def _backup(self, interface):
        local_timestamp = snapshot_datetime(self.snapshot)
        try:
            last_log = self.get_last_log()
        except CalledProcessError as error:
            if error.returncode == 1:   # connection error
                return False
            raise
        
        if last_log:
            last_age = (local_timestamp
                            - datetime.fromisoformat(timestamp_from_log(last_log)))
            if last_age == timedelta(0):
                print(f"{self.name}: the last local snapshot was already backed up")
                return False
            elif last_age < self.interval:
                print(f"{self.name}: last backup is only {last_age} old (< {self.interval})")
                return False
        self.mount_snapshot()
        self.interface = interface
        self.backup_thread(last_log)

    def rclone(self, *args, dry_run=None, capture=False) -> Popen or None:
        """Run rclone with the given arguments
        
        Args:
          args: command line arguments passed to rclone
          dry_run: if not None, overrides dry_run set for the instance
          capture: capture the output (in stdout attribute of return value)
        
        Returns:
          Popen
        """
        dry_run = self.dry_run if dry_run is None else dry_run
        cmd = [self.rclone_path, *args]
        if self.echo or dry_run:
            print(' '.join(map(str, cmd)))
        if not dry_run:
            return run(cmd, cwd=PATH, capture_output=capture, encoding='utf-8',
                       check=True)

    def list_files(self, path, include=None, exclude=None, recursive=True,
                   dirs_only=False, files_only=False):
        args = ((['--dirs-only'] if dirs_only else [])
                + (['--files-only'] if files_only else [])
                + (['--include', include] if include else [])
                + (['--exclude', exclude] if exclude else [])
                + (['--recursive'] if recursive else []))
        list_cmd = self.rclone('lsf', path, '--dir-slash=false', *args,
                               dry_run=False, capture=True)
        return sorted(list_cmd.stdout.split())

    def get_last_log(self):
        logs = self.list_files(self.destination, include=f"/{self.name}_*_sync.log",
                               recursive=False, files_only=True)
        if not logs:  # this is the first backup
            return None
        try:
            last_log, = logs
        except ValueError:
            raise SystemExit(f"There should only be a single sync log file in"
                             f" {self.destination}!")
        return last_log

    def backup_thread(self, last_log):
        self.interface.prepare_(self.name)
        self.tree, large_entries = self.backup_scout()
        backup_size = self.tree.size
        if backup_size > 0:
            if large_entries:
                with self.large_files_path.open('w') as f:
                    for entry in large_entries:
                        size = format_size(entry.size, True)
                        print(f'{size}   {entry.path}', file=f)
                # the following returns when the user chooses to continue the backup
                exclude = self.interface.thresholdExceeded_(
                    (self.name, backup_size, large_entries,
                     self.large_files_path, self.exclude_file,
                     self.ncdu_export_path))
                backup_size -= sum(entry.size for entry in exclude)
            else:
                exclude = []
            self.interface.startBackup_((self.name, backup_size))
            backup_dir = (self.destination / timestamp_from_log(last_log)
                        if last_log else None)
            success = self.backup_sync(backup_dir, exclude)
            if backup_dir:
                # move the logs from the last backup to the backup dir
                last_logs = '/' + last_log.replace('sync.log', '*.*')
                self.rclone('move', self.destination, '--include', last_logs,
                            backup_dir)
                self.record_backup_size(backup_dir)
            # copy logs for this backup to the remote
            local_logs = self.file_path('*', '*')
            self.rclone('copy', local_logs.parent, '--include', local_logs.name,
                        self.destination)
        self.interface.idle_()
        self.cleanup()

    def sync_popen(self, *args, dry_run=False):
        snapshot_source = self.mount_point / self.source.relative_to('/')
        extra = list(chain(['--bwlimit', self.bwlimit] if self.bwlimit else [],
                           ['--dry-run'] if dry_run else [],
                           ['--progress'] if self.echo else []))
        cmd = [self.rclone_path, 'sync', '--use-json-log', '--fast-list',
               '--links', '--track-renames', '--track-renames-strategy',
               'modtime,leaf', *args, *extra, snapshot_source,
               self.destination_latest]
        if self.echo:
            print(' '.join(map(str, cmd)))
        return Popen(cmd, stderr=PIPE)

    def backup_scout(self):
        tree = Directory('')
        exclude = (['--exclude-from', self.exclude_file]
                   if self.exclude_file.exists() else [])
        try:
            rclone_sync = self.sync_popen(*exclude, dry_run=True)
            scout_log = self.file_path('scout', 'log')
            with scout_log.open('wb') as log:
                for line in rclone_sync.stderr:
                    log.write(line)
                    msg = json.loads(line)
                    if msg.get('skipped') == 'copy':
                        rel_path = Path(msg['object'])
                        tree.add_file(rel_path, msg['size'])
        except CalledProcessError as cpe:
            # TODO: interpret rclone_sync.returncode
            raise
        tree.calculate_size()
        self.tree = tree
        write_ncdu_export(self.source, tree, self.ncdu_export_path)
        return tree, sorted(find_large_entries(tree, self.threshold),
                            key=lambda item: item.size, reverse=True)

        # TODO: caffeinate
        # FIXME: abort subprocesses on App quit
    def backup_sync(self, backup_dir, exclude):
        files_txt = self.file_path('files', 'txt')
        with files_txt.open('w') as files:
            for file in self.tree.iter_files(exclude=exclude):
                print(file.path, file=files)
        files_txt = self.file_path('files', 'txt')
        backupdir_args = ['--backup-dir', backup_dir] if backup_dir else []
        try:
            sync = self.sync_popen('--log-level', 'INFO',
                                   '--files-from-raw', files_txt,
                                   *backupdir_args, dry_run=self.dry_run)
            transferred = 0
            sync_log = self.file_path('sync', 'log')
            with sync_log.open('wb') as log:
                for line in sync.stderr:
                    log.write(line)
                    msg = json.loads(line)
                    if size := self._get_item_size(msg):
                        transferred += size
                        self.interface.updateProgress_(transferred)
                    elif msg['level'] == 'error':
                        print('ERROR:', msg['msg'])
        except CalledProcessError as cpe:
            rc = cpe.returncode
            info = RCLONE_EXIT_CODES[rc]
            print(f"rclone returned non-zero exit status {rc} - {info}")
            print(f"The log file is {sync_log}")
            return False
        return True

    def _get_item_size(self, log_msg):
        if log_msg['msg'].startswith('Copied'):
            return self.tree.get(log_msg['object']).size
        elif self.dry_run and log_msg.get('skipped') == 'copy':
            return log_msg.get('size')

    def record_backup_size(self, backupdir):
        size_cmd = self.rclone('size', '--json', backupdir, capture=True)
        size = 0 if self.dry_run else json.loads(size_cmd.stdout)['bytes']
        size_filename = f'{self.name}_{backupdir.name}_size_{size}'
        self.rclone('touch', backupdir / size_filename)

    RE_SIZE = re.compile(r"^(.+)\/.*_\1_size_(\d+)$")

    def print_snapshot_sizes(self):
        size_files = self.list_files(self.destination,
                                    include=f"/*/{self.name}_*_size_*",
                                    files_only=True)
        total = 0
        print(f"Size of snapshots in {self.destination}")
        for path in size_files:
            timestamp, size_str = self.RE_SIZE.match(path).groups()
            size = int(size_str)
            print(f"{timestamp:18} {format_size(size, True):>12}")
            total += size
        print(f"{'Total:':18} {format_size(total, True):>12}")


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


from AppKit import NSAttributedString
from Cocoa import NSColor, NSForegroundColorAttributeName
from Foundation import NSObject
from PyObjCTools.Conversion import propertyListFromPythonCollection

from queue import Queue


class AppInterface(NSObject):

    def __new__(cls, *args, **kwargs):
        # https://pyobjc.readthedocs.io/en/latest/examples/Cocoa/AppKit/PythonBrowser/index.html
        return cls.alloc().init()

    def __init__(self, app):
        self.app = app
        self._queue = Queue(maxsize=1)

    # process -> app
    
    def idle_(self, _=None):
        self.pyobjc_performSelectorOnMainThread_withObject_('_idle:', None)
    
    def _idle_(self, _):
        self.app.idle()
        
    def prepare_(self, backup_name):
        self.pyobjc_performSelectorOnMainThread_withObject_('_prepare:', backup_name)

    def _prepare_(self, backup_name):
        self.app.prepare(backup_name)
    
    def thresholdExceeded_(self, args):
        self.pyobjc_performSelectorOnMainThread_withObject_('_thresholdExceeded:', args)
        return self._queue.get()

    def _thresholdExceeded_(self, args):
        self.app.threshold_exceeded(*args)

    def startBackup_(self, args):
        self.pyobjc_performSelectorOnMainThread_withObject_('_startBackup:', args)

    def _startBackup_(self, args):
        self.app.start_backup(*args)

    def updateProgress_(self, transferred_bytes):
        self.pyobjc_performSelectorOnMainThread_withObject_('_updateProgress:', transferred_bytes)
        
    def _updateProgress_(self, transferred_bytes):
        self.app.update_progress(transferred_bytes)
    
    def quitApp_(self, success):
        self.pyobjc_performSelectorOnMainThread_withObject_('_quitApp:', success)

    def _quitApp_(self, success):
        if not success:
            raise SystemExit(1)
        self.app.quit()

    # app -> process
        
    def continueBackup_(self, excluded):
        self._queue.put_nowait(excluded)

    def abortBackup(self):
        # ALT: just kill rclone process from this thread? (call Popen.terminate())
        self.pyobjc_performSelector_onThread_withObject_waitUntilDone_(
            '_abortBackup:', self.process.thread, None, False) # probably only takes NSThread

    def _abortBackup_(self, _):
        self.process
        
    def cleanUp(self):
        self.process.cleanup()


def main_loop(interface, echo, dry_run):
    while True:
        config = Configuration(PATH / 'backups.toml',
                               echo=echo, dry_run=dry_run)
        for backup in config.values():
            if backup.backup(interface):
                break   # only continue to next backup if current one is skipped
        time.sleep(15 * 60)


class MenuBarApp(rumps.App):
    
    def __init__(self, echo=False, dry_run=False):
        super().__init__('rclone backup', icon='rclone.icns', template=True,
                         quit_button=None)
        self.backup_name = None
        self.total_size = None
        self.large_files_path = None
        self.exclude_file = None
        self.ncdu_export_path = None
        self.large_entry_menu_items = []
        self.total_size_menu_item = None
        self.progress_menu_item = None
        self.idle()
        self.interface = AppInterface(self)
        self.thread = Thread(target=main_loop,
                             args=[self.interface, echo, dry_run], daemon=True)
        self.thread.start()

    def add_menuitem(self, title, callback=None, key=None):
        menu_item = rumps.MenuItem(title, callback=callback, key=key)
        self.menu.add(menu_item)
        return menu_item

    def add_show_files_file_menu_item(self):
        self.add_menuitem('Show Files', self.show_files, 'f')

    def idle(self):
        self.title = None
        self.menu.clear()
        self.add_menuitem('Quit', rumps.quit_application, 'q')
        self.backup_name = None
        self.exclude_file = None
        self.ncdu_export_path = None

    def prepare(self, backup_name):
        self.add_menuitem(f'{backup_name}: determining backup size...')

    def set_title(self, title, color=None):
        self.title = f' {title}'
        if color:   # https://github.com/jaredks/rumps/issues/30
            r, g, b, a = color
            color = NSColor.colorWithCalibratedRed_green_blue_alpha_(r, g, b, a)
            attributes = propertyListFromPythonCollection({NSForegroundColorAttributeName: color}, conversionHelper=lambda x: x)
            string = NSAttributedString.alloc().initWithString_attributes_(self.title, attributes)
            self._nsapp.nsstatusitem.setAttributedTitle_(string)

    def threshold_exceeded(self, backup_name, total_size, large_entries,
                           large_files_path, exclude_file, ncdu_export_path):
        self.menu.clear()
        self.total_size = total_size
        self.large_files_path = large_files_path
        self.exclude_file = exclude_file
        self.ncdu_export_path = ncdu_export_path
        rumps.notification(f"{backup_name}: Backup size exceeds treshold", None,
                           f"Total backup size: {format_size(total_size)}")
        self.set_title(f"{backup_name} {format_size(total_size)}",
                       color=(1, 0, 0, 1))
        self.add_menuitem('Continue Backup', self.continue_backup, 'c')
        self.add_menuitem('Skip Backup', self.skip_backup, 's')
        self.add_menuitem('Edit Exclude File', self.edit_exclude_file, 'x')
        self.add_show_files_file_menu_item()
        self.menu.add(rumps.separator)
        self.add_menuitem('Items excluded from backup (check to include):')
        for i, entry in enumerate(large_entries, start=1):
            self.add_large_menu_item(entry, i)
        self.total_size_menu_item = self.add_menuitem('')
        self.update_backup_size()

    def add_large_menu_item(self, entry, index):
        menu_item = rumps.MenuItem(
            f'{format_size(entry.size, True)}  {entry.path}',
            key=str(index) if index < 10 else None,
            callback=lambda menu_item:
                self.large_entry_menu_item_clicked(menu_item, str(entry.path))
        )
        self.menu.add(menu_item)
        self.large_entry_menu_items.append((menu_item, entry))

    def large_entry_menu_item_clicked(self, menu_item, path):
        menu_item.state = not menu_item.state
        run('pbcopy', env={'LANG': 'en_US.UTF-8'}, input=path, text=True)
        self.update_backup_size()

    def update_backup_size(self):
        excluded_size = sum(entry.size
                            for menu_item, entry in self.large_entry_menu_items
                            if not menu_item.state)
        size = self.total_size - excluded_size
        self.total_size_menu_item.title = f'Backup size: {format_size(size)}'

    def continue_backup(self, _):
        exclude = []
        for menu_item, entry in self.large_entry_menu_items:
            if menu_item.state:
                print(f'keep {entry.path} ({format_size(entry.size)})')
            else:
                exclude.append(entry)
        self.interface.continueBackup_(exclude)

    def start_backup(self, backup_name, total_bytes):
        self.backup_name = backup_name
        self.total_bytes = total_bytes
        self.menu.clear()
        self.progress_menu_item = self.add_menuitem('')
        self.add_show_files_file_menu_item()
        self.add_menuitem('Abort Backup', self.abort_backup, 'a')
        self.set_title(format_size(total_bytes))

    def update_progress(self, transferred):
        self.progress_menu_item.title = \
            (f'{self.backup_name} {format_size(transferred)}'
             f' of {format_size(self.total_bytes)}')
        self.set_title(f'{transferred / self.total_bytes:.0%}')

    # TODO: extra menu entries:
    # - backup everything
    # - continue but exclude ml dirs/files

    def skip_backup(self, _):
        self.quit()

    def edit_exclude_file(self, _):
        Popen(['qlmanage', '-p', self.large_files_path], stderr=DEVNULL)
        run(['open', '-a', 'TextEdit', self.exclude_file])

    def show_files(self, _):
        script = TERMINAL_NCDU.format(file=self.ncdu_export_path)
        run(['osascript', '-e', script])

    def abort_backup(self, _):
        self.interface.abortBackup()
        self.quit()
        
    def quit(self):
        self.interface.cleanUp()
        rumps.quit_application()


TERMINAL_NCDU = """
tell app "Terminal"
  do script "ncdu --color off --apparent-size -f {file}; exit"
  set current settings of first window to settings set "ocean"
  activate
end tell
"""

RE_KEEP = re.compile(r'(?P<days>\d+)\s*(d(ays?)?)?', re.IGNORECASE)

RE_INTERVAL = re.compile(r'((?P<days>\d+?)\s*(d|days?))?\s*'
                         r'((?P<hours>\d+?)\s*(h|hours?))?\s*'
                         r'((?P<minutes>\d+?)\s*(m|minutes?))?', re.IGNORECASE)

RE_THRESHOLD = re.compile(r'(?P<number>\d+)\s*(?P<unit>[KMGT])B?',
                          re.IGNORECASE)


class Configuration(dict):
    def __init__(self, config_path, echo, dry_run) -> None:
        self.config_path = config_path
        self.echo = echo
        self.dry_run = dry_run
        with config_path.open('rb') as f:
            self.toml = tomllib.load(f)
        self.rclone = self.toml.get('rclone', 'rclone')
        self.bwlimit = self.toml.get('bwlimit', None)
        self.keep_all = self._parse_keep('keep_all', '7 days')
        self.keep_daily = self._parse_keep('keep_daily', '31 days')
        for key, value in self.toml.items():
            if isinstance(value, dict):
                self[key] = self._create_backup(key, value)

    def _syntax_error(self, attribute, section=None):
        sect = f' in section [{section}]' if section else ''
        raise SystemExit(f"{self.config_path.name}{sect}: value for"
                         f" '{attribute}' could not be parsed")

    def _parse_keep(self, attribute, default):
        value = self.toml.get('keep_all', default)
        keep_match = RE_KEEP.fullmatch(value.strip())
        try:
            return int(keep_match.group('days'))
        except AttributeError:
            self._syntax_error(attribute)

    def _create_backup(self, name, cfg):
        src, dest = cfg['source'], cfg['destination']
        interval_match = RE_INTERVAL.fullmatch(cfg['interval'].strip())
        try:
            interval = timedelta(**{key: int(value) for key, value in
                                    interval_match.groupdict(0).items()})
        except AttributeError:
            self._syntax_error('interval', section=name)
        threshold = cfg.get('threshold')
        if threshold:
            try:
                threshold_match = RE_THRESHOLD.fullmatch(threshold.strip())
                exp = EXPONENTS[threshold_match.group('unit')]
                threshold = int(threshold_match.group('number')) * 2**exp
            except TypeError:
                pass    # threshold is a number
            except AttributeError:
                self._syntax_error('threshold', section=name)
        return RCloneBackup(name, src, dest, interval, threshold,
                            bwlimit=self.bwlimit, rclone=self.rclone,
                            echo=self.echo, dry_run=self.dry_run)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--echo', action='store_true',
                        help="Echo the rclone commands before executing them")
    parser.add_argument('--dry-run', action='store_true',
                        help="Dry-run rclone commands")
    subparsers = parser.add_subparsers(dest='command', )#help='')
    parser_backup = subparsers.add_parser('backup', help='start a backup')
    parser_backup.add_argument('name', nargs='*',
                               help="The name of a backup configuration")
    parser_backup.add_argument('-f', '--force', action='store_true',
                               help="Force a backup regardless of when the last"
                                    " backup was performed")
    parser_list = subparsers.add_parser('list', help='list backup snapshots')
    parser_list.add_argument('backup', help="The backup configuration for which"
                                            " to list snapshots")
    args = parser.parse_args()

    with pid.PidFile(piddir=PATH):
        app = MenuBarApp(args.echo, args.dry_run)
        app.run()

    # FIXME: never reached because app just exits program; handle another way
    match args.command:
        case 'backup':
            names = args.name or config.keys()
            for name in names:
                backup = config[name]
                try:
                    if backup.backup(force=args.force):
                        break   # only continue to next backup if current one is skipped
                except pid.PidFileError:
                    if sys.stdout.isatty():
                        raise SystemExit("An rclone backup is already in progress")
        case 'list':
            if args.backup not in config:
                msg = (f"There is no backup named '{args.backup}'. Choose one"
                       f" from:\n" + '\n'.join(f'- {n}' for n in config))
                raise SystemExit(msg)
            backup = config[args.backup]
            backup.print_snapshot_sizes()
        case _:
            parser.print_help()
