

import argparse
import json
import plistlib
import re
import sys
import tomllib

from datetime import timedelta
from pathlib import Path
from subprocess import run, Popen, PIPE
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
    
    def add_file(self, path, size):
        parts = path.parts
        self._add_file(path, parts, size)

    def _add_file(self, path, path_parts, size):
        name, *parts = path_parts
        dir_path = '/'.join(path.parts[:-len(parts)])
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
        yield [], entry
    else:                   # entry is a directory
        bleh = True
        for name, e in entries.items():
            for parts, entry in find_large_entries(e, threshold):
                yield [name, *parts], entry
                bleh = False
        if bleh:
            yield [''], entry


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

    def __init__(self, name, source, destination, interval, threshold):
        self.name = name
        self.source = Path(source)
        self.destination = Path(destination)
        self.interval = interval
        self.threshold = threshold
        self.pid = None
        self.interface = None
        
        self._tempdir = TemporaryDirectory()
        self.mount_point = Path(self._tempdir.name)
        self.timestamp = self.mount_last_snapshot()        
        self.snapshot_source = self.mount_point / self.source
        self.destination_latest = self.destination / 'latest'        
        self.thread = None

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

    def __del__(self):
        # TODO: stop thread
        self.unmount_snapshot()
        
    def backup(self):
        with pid.PidFile(piddir=PATH) as self.pid:
            self.interface = AppInterface(self)
            self.thread = Thread(target=self.backup_thread, daemon=True)
            self.thread.start()
            self.interface.start_app()
        
    def mount_last_snapshot(self):
        # TODO: manually create new snapshot?
        du_info = run(['diskutil', 'info', '-plist', 'Data'],
                      check=True, capture_output=True).stdout
        device = plistlib.loads(du_info)['DeviceIdentifier']
        snapshots = run(['diskutil', 'apfs', 'listSnapshots', '-plist', device],
                        check=True, capture_output=True).stdout
        last_snapshot = plistlib.loads(snapshots)['Snapshots'][-1]['SnapshotName']
        snapshot_timestamp = RE_SNAPSHOT.match(last_snapshot).group(1)
        print('Mounting', last_snapshot, 'at', self.mount_point)
        run(['mount_apfs', '-s', last_snapshot, f'/dev/{device}', self.mount_point],
             check=True)
        return snapshot_timestamp
        
    def unmount_snapshot(self):
        run(['umount', self.mount_point], check=True)        

    def backup_thread(self):
        self.tree, large_entries = self.backup_scout()
        backup_size = self.tree.size
        if large_entries:
            # the following returns when the user chooses to continue the backup
            exclude = self.interface.thresholdExceeded_((backup_size,
                                                         large_entries))
            for entry in exclude:
                print(format_size(entry.size, True), entry.path)
            backup_size -= sum(entry.size for entry in exclude)
        else:
            exclude = []
        self.interface.startBackup_(backup_size)
        self.backup_sync(exclude)
        self.unmount_snapshot()
        self.interface.quitApp()

    def sync_popen(self, *args):
        cmd = ['rclone', 'sync', '--use-json-log', '--fast-list', '--links',
               '--track-renames', '--track-renames-strategy', 'modtime,leaf',
               *args,
               self.snapshot_source, self.destination_latest]
        return Popen(cmd, stderr=PIPE)

    def backup_scout(self):
        tree = Directory('')
        rclone_sync = self.sync_popen('--dry-run', '--progress',
                                      '--exclude-from', self.exclude_file)
        scout_log = self.file_path('scout', 'log')
        with scout_log.open('wb') as log:
            for line in rclone_sync.stderr:
                log.write(line)
                msg = json.loads(line)
                if msg.get('skipped') == 'copy':
                    rel_path = Path(msg['object'])
                    tree.add_file(rel_path, msg['size'])
        tree.calculate_size()
        self.tree = tree
        write_ncdu_export(self.source, tree, self.ncdu_export_path)
        return tree, sorted(find_large_entries(tree, self.threshold),
                            key=lambda item: item[1].size, reverse=True)

    def backup_sync(self, exclude):
        # TODO: caffeinate
        # FIXME: abort subprocesses on App quit
        files_txt = self.file_path('files', 'txt')
        with files_txt.open('w') as files:
            for file in self.tree.iter_files(exclude=exclude):
                print(file.path, file=files)
        files_txt = self.file_path('files', 'txt')
        rclone_sync = self.sync_popen('--dry-run', '--progress',
                                      '--files-from-raw', files_txt)
        transferred = 0
        sync_log = self.file_path('sync', 'log')
        with sync_log.open('wb') as log:
            for line in rclone_sync.stderr:
                log.write(line)
                msg = json.loads(line)
                if size := msg.get('size'):
                    transferred += size
                    self.interface.updateProgress_(transferred)


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
        self.add_menuitem('Preparing backup...')

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
        for i, (parts, entry) in enumerate(large_entries, start=1):
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
        run(['open', '-a', 'TextMate', self.interface.process.exclude_file])

    def show_files(self, _):
        script = TERMINAL_NCDU.format(file=self.interface.process.ncdu_export_path)
        run(['osascript', '-e', script])

    def abort_backup(self, _):
        self.interface.abortBackup()
        self.quit()
        
    def quit(self):
        self.interface.process.pid.close()
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


RE_INTERVAL = re.compile(r'((?P<days>\d+?)\s*(d|days?))?\s*'
                         r'((?P<hours>\d+?)\s*(h|hours?))?')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--echo', action='store_true',
                        help="Echo the rclone commands before executing them")
    parser.add_argument('--dry-run', action='store_true',
                        help="Dry-run rclone commands")
    subparsers = parser.add_subparsers(dest='command', )#help='')
    parser_backup = subparsers.add_parser('backup', help='start a backup')
    parser_backup.add_argument('--force', action='store_true',
                               help="Force a backup regardless of when the last"
                                    " backup was performed")
    parser_list = subparsers.add_parser('list', help='list backup snapshots')
    parser_list.add_argument('backup', 
                             help="The backup configuration for which to list"
                                  " snapshots")
    args = parser.parse_args()
    
    with (PATH / 'backups.toml').open('rb') as f:
       config = tomllib.load(f)
    backups = {key: value for key, value in config.items()
               if isinstance(value, dict)}

    match args.command:
        case 'backup':
            for name, cfg in backups.items():
                interval_match = RE_INTERVAL.fullmatch(cfg['interval'].strip())
                interval = timedelta(**{key: int(value) for key, value in
                                        interval_match.groupdict(0).items()})
                try:
                    backup = RCloneBackup(name, cfg['source'], cfg['destination'],
                                          interval, cfg['threshold'])
                    backup.backup()
                except pid.PidFileError:
                    if sys.stdout.isatty():
                        raise SystemExit("An rclone backup is already in progress")
                break
        case 'list':
            raise NotImplementedError
        case _:
            raise NotImplementedError
