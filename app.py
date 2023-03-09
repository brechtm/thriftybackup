

import json
import plistlib
import re

from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from subprocess import run, Popen, PIPE
from tempfile import TemporaryDirectory
from threading import Thread, Event

import rumps

from rumps import MenuItem


PATH = Path(__file__).parent
BACKUPS_JSON = PATH / 'backups.json'


class Entry:
    def __init__(self, size=None):
        self.size = size

    def calculate_size(self):
        raise NotImplementedError

    def to_ncdu(self, name):
        raise NotImplementedError
        
        
class File(Entry):
    def calculate_size(self):
        return self.size

    def to_ncdu(self, name):
        return dict(name=name, asize=self.size)
        

class Directory(Entry):
    def __init__(self):
        super().__init__()
        self.entries = {}
        
    def add_file(self, path_parts, size):
        name, *parts = path_parts
        if parts:
            dir = self.entries.setdefault(name, Directory())
            dir.add_file(parts, size)
        else:
            assert name not in self.entries
            self.entries[name] = File(size)

    def calculate_size(self):
        self.size = sum((e.calculate_size() for e in self.entries.values()),
                        start=0)
        return self.size

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


def backup_diff(source, destination, exclude_file):
    tree = Directory()
    rclone = Popen(['rclone', 'sync', '--dry-run', '--progress', '--use-json-log',
                    '--exclude-from', exclude_file, '--fast-list', '--links',
                    '--track-renames', '--track-renames-strategy', 'modtime,leaf',
                    source, destination], stderr=PIPE)
    for line in rclone.stderr:
        msg = json.loads(line)
        if msg.get('skipped') == 'copy':
            rel_path = Path(msg['object'])
            tree.add_file(rel_path.parts, msg['size'])
    tree.calculate_size()
    return tree


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


def write_ncdu_export(tree, ncdu_export_path):
    ncdu = [1, 2, dict(progname=__file__, progver='0.0.0', timestamp=0),
            tree.to_ncdu(backup['source'])]
    with ncdu_export_path.open('w') as f:
        json.dump(ncdu, f)


def large_entry_menu_item_clicked(menu_item, path):
    menu_item.state = not menu_item.state
    process = run( # https://stackoverflow.com/a/25802742
        'pbcopy', env={'LANG': 'en_US.UTF-8'}, input=path, text=True)


# phases:
# 0) check whether current snapshot is already backed up
# 1) prepare: determine size of backups and check threshold
# 2) wait for user feedback
# 3) backup

RE_SNAPSHOT = re.compile('com\.apple\.TimeMachine\.(\d{4}-\d{2}-\d{2}-\d{6})\.local')


class BackupProcess:

    def __init__(self, source, destination, threshold):
        self.source = Path(source)
        self.destination = Path(destination)
        self.threshold = threshold
        self.interface = None
        
        self._tempdir = TemporaryDirectory()
        self.mount_point = Path(self._tempdir.name)
        timestamp = self.mount_last_snapshot()

        label = self.destination.name
        self.exclude_file = PATH / f'{label}.exclude'
        self.ncdu_export = PATH / 'logs' / label / f'{label}_{timestamp}.json'
        
        self.thread = None

    def __del__(self):
        # TODO: stop thread
        self.unmount_snapshot()
        
    def start(self):
        assert self.interface is not None
        self.thread = Thread(target=self.run, daemon=True)
        self.thread.start()
        
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

    def run(self):
        self.tree, large_entries = self.prepare()
        backup_size = self.tree.size
        if large_entries:
            # the following returns when the user chooses to continue the backup
            exclude = self.interface.thresholdExceeded_((backup_size,
                                                         large_entries))
            backup_size -= sum(entry.size for path, entry in exclude)
        else:
            exclude = []
        self.interface.startBackup_(backup_size)
        self.backup(exclude)
        self.unmount_snapshot()
        self.interface.quitApp()

    def prepare(self):
        source = self.mount_point / self.source
        destination = self.destination / 'latest'
        tree = backup_diff(source, destination, self.exclude_file)
        write_ncdu_export(tree, self.ncdu_export)
        return tree, sorted(find_large_entries(tree, self.threshold),
                            key=lambda item: item[1].size, reverse=True)

    def backup(self, exclude):
        # TODO: caffeinate
        # FIXME: abort subprocesses on App quit
        exclude_args = []
        for path, entry in exclude:
            exclude_args.extend(['--exclude', path])
        source = self.mount_point / self.source
        destination = self.destination / 'latest'
        rclone = Popen(['rclone', 'sync', '--dry-run', '--use-json-log',
                        '--exclude-from', self.exclude_file, *exclude_args,
                        '--fast-list', '--links', '--track-renames',
                        '--track-renames-strategy', 'modtime,leaf',
                        source, destination], stderr=PIPE)
        transferred = 0
        for line in rclone.stderr:
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

    def __init__(self, process, app):
        self.process = process
        self.app = app
        self._queue = Queue(maxsize=1)

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
    
    def __init__(self):
        super().__init__('rclone backup', icon='rclone.icns', template=True,
                         quit_button=None)
        self.large_entry_menu_items = []
        self.prepare()

    def add_menuitem(self, title, callback=None, key=None):
        self.menu.add(MenuItem(title, callback=callback, key=key))

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
            path = '/'.join(parts)
            self.add_large_menu_item(path, entry, i)

    def add_large_menu_item(self, path, entry, index):
        menu_item = MenuItem(f'{format_size(entry.size, True)}  {path}',
                             key=str(index) if index < 10 else None,
                             callback=lambda menu_item:
                                 large_entry_menu_item_clicked(menu_item, path))
        self.menu.add(menu_item)
        self.large_entry_menu_items.append((menu_item, path, entry))

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

    def continue_backup(self, _):
        exclude = []
        for menu_item, path, entry in self.large_entry_menu_items:
            if menu_item.state:
                print(f'keep {path} ({format_size(entry.size)})')
            else:
                exclude.append((path, entry))
        self.interface.continueBackup_(exclude)

    # TODO: extra menu entries:
    # - backup everything
    # - continue but exclude ml dirs/files

    def skip_backup(self, _):
        self.quit()

    def edit_exclude_file(self, _):
        run(['open', '-a', 'TextMate', self.interface.process.exclude_file])

    def show_files(self, _):
        script = TERMINAL_NCDU.format(file=self.interface.process.ncdu_export)
        run(['osascript', '-e', script])

    def abort_backup(self, _):
        self.interface.abortBackup()
        self.quit()
        
    def quit(self):
        rumps.quit_application()


TERMINAL_NCDU = """
tell app "Terminal"
  do script "ncdu --color off --apparent-size -f {file}; exit"
  set current settings of first window to settings set "ocean"
  activate
end tell
"""

if __name__ == '__main__':
    with BACKUPS_JSON.open() as f:
        backups = json.load(f)
    for backup in backups:
        process = BackupProcess(backup['source'], backup['destination'],
                                backup['threshold'])
        app = MenuBarApp()
        interface = AppInterface(process, app)
        process.interface = app.interface = interface
        process.start()
        app.run()
        break
