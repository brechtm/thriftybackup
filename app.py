

import argparse
import json
import plistlib
import re
import sys
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
        parts = path.split('/')
        return 

    def _get(self, parts):
        name, *rest = parts
        return self.entries[name]._get(rest) if parts else self.entries[name]

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


class RCloneBackup:

    def __init__(self, name, source, destination, interval, threshold,
                 bwlimit=None, echo=False, dry_run=False):
        self.name = name
        self.source = Path(source)
        self.destination = Path(destination)
        self.interval = interval
        self.threshold = threshold
        self.bwlimit = bwlimit
        self.pid = None
        self.interface = None
        self.tree = None
        
        self.echo = True        # TODO: replace when things are working
        self.dry_run = True     # TODO: replace when things are working

        self.device, self.snapshot = self.get_last_snapshot()
        self.timestamp = RE_SNAPSHOT.match(self.snapshot).group(1)
        self._tempdir = None
        self.mount_point = None

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
        self.pid.close()

    def get_last_snapshot(self):
        du_info = run(['diskutil', 'info', '-plist', 'Data'],
                      check=True, capture_output=True).stdout
        device = plistlib.loads(du_info)['DeviceIdentifier']
        snapshots = run(['diskutil', 'apfs', 'listSnapshots', '-plist', device],
                        check=True, capture_output=True).stdout
        snapshot = plistlib.loads(snapshots)['Snapshots'][-1]['SnapshotName']
        return device, snapshot

    def mount_snapshot(self):
        self._tempdir = TemporaryDirectory()
        self.mount_point = Path(self._tempdir.name)
        print(f'Mounting {self.snapshot} at {self.mount_point}')
        run(['mount_apfs', '-s', self.snapshot,
             f'/dev/{self.device}', self.mount_point], check=True)
        
    def unmount_snapshot(self):
        run(['umount', self.mount_point], check=True)        

    def backup(self, force=False):
        with pid.PidFile(piddir=PATH) as self.pid:
            return self._backup(force)

    def _backup(self, force):
        local_timestamp = datetime.fromisoformat(self.timestamp)
        try:
            last_log = self.get_last_log()
        except CalledProcessError as error:
            if error.returncode == 1:   # connection error
                return False
            raise
        
        if last_log:
            last_age = (local_timestamp
                            - datetime.fromisoformat(timestamp_from_log(last_log)))
            forced = force and last_age > timedelta(0)   # there must be changes
            if last_age < self.interval and not forced:
                print(f"Last backup is only {last_age} old (< {self.interval})")
                return False
        self.mount_snapshot()
        self.interface = AppInterface(self)
        self.thread = Thread(target=self.backup_thread, args=[last_log],
                             daemon=True)
        self.thread.start()
        self.interface.start_app()

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
        cmd = ['rclone', *args]
        if self.echo or dry_run:
            print(' '.join(map(str, cmd)))
        if not dry_run:
            return run(cmd, cwd=PATH, capture_output=capture, encoding="utf-8",
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
        logs = self.list_files(self.destination, include=f"/{self.name}_*.log",
                               recursive=False, files_only=True)
        if not logs:  # this is the first backup
            return None, None
        try:
            last_log, = logs
        except ValueError:
            raise SystemExit(f"There should only be a single sync log file in"
                             f" {self.destination}!")
        return last_log

    def backup_thread(self, last_log):
        self.tree, large_entries = self.backup_scout()
        backup_size = self.tree.size
        if large_entries:
            with self.large_files_path.open('w') as f:
                for entry in large_entries:
                    size = format_size(entry.size, True)
                    print(f'{size}   {entry.path}', file=f)
            # the following returns when the user chooses to continue the backup
            exclude = self.interface.thresholdExceeded_((backup_size,
                                                         large_entries))
            backup_size -= sum(entry.size for entry in exclude)
        else:
            exclude = []
        self.interface.startBackup_(backup_size)
        backup_dir = (self.destination / timestamp_from_log(last_log)
                      if last_log else None)
        sync_log = self.backup_sync(backup_dir, exclude)
        if backup_dir:
            last_logs = '/' + last_log.replace('sync.log', '*.*')
            self.rclone('move', self.destination, '--include', last_logs,
                        backup_dir)
            local_logs = self.file_path('*', '*')
            self.rclone('copy', local_logs.parent, '--include',
                        local_logs.name, self.destination)
            self.record_backup_size(backup_dir)
        self.cleanup()
        self.interface.quitApp()

    def sync_popen(self, *args, dry_run=False):
        snapshot_source = self.mount_point / self.source.relative_to('/')
        destination = self.destination / 'latest'
        extra = list(chain(['--bwlimit', self.bwlimit] if self.bwlimit else [],
                           ['--dry-run'] if dry_run else [],
                           ['--progress'] if self.echo else []))
        cmd = ['rclone', 'sync', '--use-json-log', '--fast-list', '--links',
               '--track-renames', '--track-renames-strategy', 'modtime,leaf',
               '--retries', '1', *args, *extra, snapshot_source, destination]
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
        except CalledProcessError as cpe:
            raise
        return sync_log

    def _get_item_size(self, log_msg):
        if log_msg['msg'].startswith('Copied'):
            return self.tree.get(log_msg['object']).size
        elif self.dry_run and log_msg.get('skipped') == 'copy':
            return log_msg.get('size')

    def record_backup_size(self, backupdir):
        size_cmd = self.rclone('size', '--json', '--exclude',
                               f'/{self.name}_*.log', backupdir, capture=True)
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


from AppKit import NSAttributedString
from Cocoa import NSColor, NSForegroundColorAttributeName
from Foundation import NSObject
from PyObjCTools.Conversion import propertyListFromPythonCollection

from queue import Queue


class AppInterface(NSObject):

    def __new__(cls, *args, **kwargs):
        # https://pyobjc.readthedocs.io/en/latest/examples/Cocoa/AppKit/PythonBrowser/index.html
        return cls.alloc().init()

    def __init__(self, process):
        self.process = process
        self.app = MenuBarApp(self)
        self._queue = Queue(maxsize=1)

    def start_app(self):
        self.app.run()

    # process -> app
    
    def thresholdExceeded_(self, args):
        self.pyobjc_performSelectorOnMainThread_withObject_('_thresholdExceeded:', args)
        return self._queue.get()

    def _thresholdExceeded_(self, args):
        self.app.threshold_exceeded(*args)

    def startBackup_(self, total_bytes):
        self.pyobjc_performSelectorOnMainThread_withObject_('_startBackup:', total_bytes)

    def _startBackup_(self, total_bytes):
        self.app.start_backup(total_bytes)        

    def updateProgress_(self, transferred_bytes):
        self.pyobjc_performSelectorOnMainThread_withObject_('_updateProgress:', transferred_bytes)
        
    def _updateProgress_(self, transferred_bytes):
        self.app.update_progress(transferred_bytes)
    
    def quitApp(self):
        self.pyobjc_performSelectorOnMainThread_withObject_('_quitApp:', None)

    def _quitApp_(self, _):
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


class MenuBarApp(rumps.App):
    
    def __init__(self, interface):
        super().__init__('rclone backup', icon='rclone.icns', template=True,
                         quit_button=None)
        self.interface = interface
        self.large_entry_menu_items = []
        self.prepare()

    def add_menuitem(self, title, callback=None, key=None):
        self.menu.add(rumps.MenuItem(title, callback=callback, key=key))

    def add_show_files_file_menu_item(self):
        self.add_menuitem('Show Files', self.show_files, 'f')

    def prepare(self):
        self.add_menuitem('Determining backup size...')

    def set_title(self, title, color=None):
        self.title = f' {title}'
        if color:   # https://github.com/jaredks/rumps/issues/30
            r, g, b, a = color
            color = NSColor.colorWithCalibratedRed_green_blue_alpha_(r, g, b, a)
            attributes = propertyListFromPythonCollection({NSForegroundColorAttributeName: color}, conversionHelper=lambda x: x)
            string = NSAttributedString.alloc().initWithString_attributes_(self.title, attributes)
            self._nsapp.nsstatusitem.setAttributedTitle_(string)

    def threshold_exceeded(self, total_size, large_entries):
        self.menu.clear()
        rumps.notification("Backup size exceeds treshold", None,
                           f"Total backup size: {format_size(total_size)}")
        self.set_title(format_size(total_size), color=(1, 0, 0, 1))
        self.add_menuitem('Continue Backup', self.continue_backup, 'c')
        self.add_menuitem('Skip Backup', self.skip_backup, 's')
        self.add_menuitem('Edit Exclude File', self.edit_exclude_file, 'x')
        self.add_show_files_file_menu_item()
        self.menu.add(rumps.separator)
        self.add_menuitem('Items excluded from backup (check to include):')
        for i, entry in enumerate(large_entries, start=1):
            self.add_large_menu_item(entry, i)

    def add_large_menu_item(self, entry, index):
        menu_item = rumps.MenuItem(
            f'{format_size(entry.size, True)}  {entry.path}',
            key=str(index) if index < 10 else None,
            callback=lambda menu_item:
                large_entry_menu_item_clicked(menu_item, str(entry.path))
        )
        self.menu.add(menu_item)
        self.large_entry_menu_items.append((menu_item, entry))

    def continue_backup(self, _):
        exclude = []
        for menu_item, entry in self.large_entry_menu_items:
            if menu_item.state:
                print(f'keep {entry.path} ({format_size(entry.size)})')
            else:
                exclude.append(entry)
        self.interface.continueBackup_(exclude)

    def start_backup(self, total_bytes):
        self.total_bytes = total_bytes
        self.menu.clear()
        self.add_show_files_file_menu_item()
        self.add_menuitem('Abort Backup', self.abort_backup, 'a')
        self.set_title(format_size(total_bytes))

    def update_progress(self, transferred):
        total = self.total_bytes
        self.set_title(f'{format_size(transferred)} of {format_size(total)}'
                       f' ({transferred / total:.0%})')

    # TODO: extra menu entries:
    # - backup everything
    # - continue but exclude ml dirs/files

    def skip_backup(self, _):
        self.quit()

    def edit_exclude_file(self, _):
        Popen(['qlmanage', '-p', self.interface.process.large_files_path],
              stderr=DEVNULL)
        run(['open', '-a', 'TextEdit', self.interface.process.exclude_file])

    def show_files(self, _):
        script = TERMINAL_NCDU.format(file=self.interface.process.ncdu_export_path)
        run(['osascript', '-e', script])

    def abort_backup(self, _):
        self.interface.abortBackup()
        self.quit()
        
    def quit(self):
        self.interface.cleanUp()
        rumps.quit_application()


def large_entry_menu_item_clicked(menu_item, path):
    menu_item.state = not menu_item.state
    # https://stackoverflow.com/a/25802742
    run('pbcopy', env={'LANG': 'en_US.UTF-8'}, input=path, text=True)


TERMINAL_NCDU = """
tell app "Terminal"
  do script "ncdu --color off --apparent-size -f {file}; exit"
  set current settings of first window to settings set "ocean"
  activate
end tell
"""

RE_KEEP = re.compile(r'(?P<days>\d+)\s*(d(ays?)?)?', re.IGNORECASE)

RE_INTERVAL = re.compile(r'((?P<days>\d+?)\s*(d|days?))?\s*'
                         r'((?P<hours>\d+?)\s*(h|hours?))?', re.IGNORECASE)

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
                            bwlimit=self.bwlimit, echo=self.echo,
                            dry_run=self.dry_run)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--echo', action='store_true',
                        help="Echo the rclone commands before executing them")
    parser.add_argument('--dry-run', action='store_true',
                        help="Dry-run rclone commands")
    subparsers = parser.add_subparsers(dest='command', )#help='')
    parser_backup = subparsers.add_parser('backup', help='start a backup')
    parser_backup.add_argument('-f', '--force', action='store_true',
                               help="Force a backup regardless of when the last"
                                    " backup was performed")
    parser_list = subparsers.add_parser('list', help='list backup snapshots')
    parser_list.add_argument('backup', help="The backup configuration for which"
                                            " to list snapshots")
    args = parser.parse_args()
    
    config = Configuration(PATH / 'backups.toml',
                           echo=args.echo, dry_run=args.dry_run)

    match args.command:
        case 'backup':
            for backup in config.values():
                try:
                    if backup.backup(force=args.force):
                        break   # only continue to next backup if current one is skipped
                except pid.PidFileError:
                    if sys.stdout.isatty():
                        raise SystemExit("An rclone backup is already in progress")
                break
        case 'list':
            if args.backup not in config:
                msg = (f"There is no backup named '{args.backup}'. Choose one"
                       f" from:\n" + '\n'.join(f'- {n}' for n in config))
                raise SystemExit(msg)
            backup = config[args.backup]
            backup.print_snapshot_sizes()
        case _:
            parser.print_help()
