
import os

from functools import partial
from pathlib import Path
from queue import Empty, Queue
from subprocess import run, Popen, DEVNULL, CalledProcessError
from threading import Thread

import rumps

from AppKit import NSAttributedString
from Cocoa import NSColor, NSForegroundColorAttributeName
from Foundation import NSObject
from PyObjCTools.Conversion import propertyListFromPythonCollection

from thriftybackup import CONFIG_DIR, CONFIG_PATH
from thriftybackup.config import Configuration, CONFIG_TEMPLATE
from thriftybackup.util import format_size


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


class AppInterface(NSObject):
    """Handles communication from RCloneBackup to MenuBarApp"""

    def __new__(cls, *args, **kwargs):
        # https://pyobjc.readthedocs.io/en/latest/examples/Cocoa/AppKit/PythonBrowser/index.html
        return cls.alloc().init()

    def __init__(self, app):
        self.app = app

    # process -> app
    
    def idle_(self, _=None):
        self.pyobjc_performSelectorOnMainThread_withObject_('_idle:', None)
    
    def _idle_(self, _):
        self.app.idle()
        
    def prepare_(self, backup):
        self.pyobjc_performSelectorOnMainThread_withObject_('_prepare:', backup)

    def _prepare_(self, backup):
        self.app.prepare(backup)
    
    def thresholdExceeded_size_largeEntries_(self, backup, backup_size, large_entries):
        args = (backup, backup_size, large_entries)
        self.pyobjc_performSelectorOnMainThread_withObject_('_thresholdExceeded:', args)

    def _thresholdExceeded_(self, args):
        self.app.threshold_exceeded(*args)

    def startBackup_size_(self, backup, backup_size):
        self.pyobjc_performSelectorOnMainThread_withObject_('_startBackup:', (backup, backup_size))

    def _startBackup_(self, args):
        self.app.start_backup(*args)

    def updateProgress_transferred_(self, backup, transferred_bytes):
        self.pyobjc_performSelectorOnMainThread_withObject_('_updateProgress:', (backup, transferred_bytes))
        
    def _updateProgress_(self, args):
        self.app.update_progress(*args)
    
    def quitApp_(self, success):
        self.pyobjc_performSelectorOnMainThread_withObject_('_quitApp:', success)

    def _quitApp_(self, success):
        if not success:
            raise SystemExit(1)
        self.app.quit()



CHECK_INTERVAL = 5 * 60     # 5 minutes


class BackupDaemon:
    """Schedules automatic backups and handles requests from the app"""
    
    def __init__(self, app, echo=False, dry_run=False) -> None:
        self.app = app
        self.echo = echo
        self.dry_run = dry_run
        self._backup_now = Queue(maxsize=1)
        self._interface = AppInterface(app)
        self._thread = Thread(target=self._main_loop)
        self._backup = None     # running backup

    def _load_configuration(self):
        return Configuration(CONFIG_PATH, echo=self.echo, dry_run=self.dry_run)

    def _main_loop(self):
        while True:
            try:
                backup = self._backup_now.get(timeout=CHECK_INTERVAL)
                if backup is None:  # app asks to quit
                    break
                backup.backup(self, self._interface, force=True)
            except Empty:
                for backup in self.configurations:
                    backup = backup
                    if backup.backup(self, self._interface):
                        break   # only continue to next backup if current one is skipped

    def start(self):
        self._thread.start()

    @property
    def configurations(self):
        config = self._load_configuration()
        yield from config.values()

    def backup_now(self, backup):
        self._backup_now.put(backup)

    def abort_backup(self):
        pass    # TODO: call Backup method that calls Popen.terminate()?
    
    def shutdown(self):
        self._backup_now.put(None)
        self._thread.join()
    

class MenuBarApp(rumps.App):
    
    def __init__(self, echo=False, dry_run=False):
        super().__init__('rclone backup', icon='rclone.icns', template=True,
                         quit_button=None)
        self.total_size = None
        self.large_entry_menu_items = []
        self.total_size_menu_item = None
        self.progress_menu_item = None
        self.daemon = BackupDaemon(self, echo, dry_run)
        self.daemon.start()
        self.idle()

    def add_menuitem(self, title, callback=None, key=None, parent=None):
        menu_item = rumps.MenuItem(title, callback=callback, key=key)
        (self.menu if parent is None else parent).add(menu_item)
        return menu_item

    def add_show_files_menu_item(self, ncdu_export_path):
        show_files = partial(self.show_files, ncdu_export_path=ncdu_export_path)
        self.add_menuitem('Show Files', show_files, 'f')

    def idle(self):
        self.title = None
        self.menu.clear()
        for backup in self.daemon.configurations:     # TODO: slow, run in thread?
            menu = self.add_menuitem(backup.name)
            backup_now = partial(self.backup_now, backup=backup)
            self.add_menuitem('Backup now', backup_now, parent=menu)
            edit_exclude = partial(self.edit_exclude_file,
                                   exclude_file=backup.exclude_file)
            self.add_menuitem('Edit exclude file', edit_exclude, parent=menu)
            menu.add(rumps.separator)
            self.add_menuitem('Last backups:', parent=menu)
            for snapshot, size in backup.last_backups():
                size_str = format_size(size, True).replace(' ', '\u2007')
                self.add_menuitem(f"{snapshot}\t{size_str}", parent=menu)   # TODO: callback=ncdu
        self.menu.add(rumps.separator)
        self.add_menuitem('Edit configuration', self.edit_config_file, ',')
        self.add_menuitem('Install command-line tool', self.install_thrifty, 'c')
        self.add_menuitem('Quit', self.quit, 'q')

    def backup_now(self, _, backup):
        self.daemon.backup_now(backup)

    def prepare(self, backup):
        self.menu.clear()
        self.add_menuitem(f'{backup.name}: determining backup size...')

    def set_title(self, title, color=None):
        self.title = f' {title}'
        if color:   # https://github.com/jaredks/rumps/issues/30
            r, g, b, a = color
            color = NSColor.colorWithCalibratedRed_green_blue_alpha_(r, g, b, a)
            attributes = propertyListFromPythonCollection({NSForegroundColorAttributeName: color}, conversionHelper=lambda x: x)
            string = NSAttributedString.alloc().initWithString_attributes_(self.title, attributes)
            self._nsapp.nsstatusitem.setAttributedTitle_(string)

    def threshold_exceeded(self, backup, total_size, large_entries):
        self.menu.clear()
        self.total_size = total_size
        rumps.notification(f"{backup.name}: Backup size exceeds treshold", None,
                           f"Total backup size: {format_size(total_size)}")
        self.set_title(f"{backup.name}: {format_size(total_size)}",
                       color=(1, 0, 0, 1))
        continue_backup = partial(self.continue_backup, backup=backup)
        self.add_menuitem('Continue Backup', continue_backup, 'c')
        skip_backup = partial(self.skip_backup, backup=backup)
        self.add_menuitem('Skip Backup', skip_backup, 's')
        edit_exclude_file = partial(self.edit_exclude_file,
                                    exclude_file=backup.exclude_file,
                                    large_files_file=backup.large_files_path)
        self.add_menuitem('Edit Exclude File', edit_exclude_file, 'x')
        self.add_show_files_menu_item(backup.ncdu_export_path)
        self.menu.add(rumps.separator)
        self.add_menuitem('Select all', self.select_all, 'a')
        self.add_menuitem('Deselect all', self.deselect_all, 'd')
        self.add_menuitem('Invert selection', self.invert_selection, 't')
        self.add_menuitem('Items excluded from backup (check to include):')
        for i, entry in enumerate(large_entries, start=1):
            self.add_large_menu_item(entry, i)
        self.total_size_menu_item = self.add_menuitem('')
        self.update_backup_size()

    def select_all(self, _):
        for menu_item, _ in self.large_entry_menu_items:
            menu_item.state = True
        self.update_backup_size()

    def deselect_all(self, _):
        for menu_item, _ in self.large_entry_menu_items:
            menu_item.state = False
        self.update_backup_size()

    def invert_selection(self, _):
        for menu_item, _ in self.large_entry_menu_items:
            menu_item.state = not menu_item.state
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

    def continue_backup(self, _, backup):
        exclude = []
        for menu_item, entry in self.large_entry_menu_items:
            if menu_item.state:
                print(f'keep {entry.path} ({format_size(entry.size)})')
            else:
                exclude.append(entry)
        backup.continue_backup(exclude)

    def skip_backup(self, _, backup):
        backup.skip_backup()

    def start_backup(self, backup, total_bytes):
        self.total_bytes = total_bytes
        self.menu.clear()
        self.progress_menu_item = self.add_menuitem('Starting backup...')
        self.add_show_files_menu_item(backup.ncdu_export_path)
        self.add_menuitem('Abort Backup', self.abort_backup, 'a')
        self.set_title(format_size(total_bytes))

    def update_progress(self, backup, transferred):
        self.progress_menu_item.title = \
            (f'{backup.name}: {format_size(transferred)}'
             f' of {format_size(self.total_bytes)}')
        self.set_title(f'{transferred / self.total_bytes:.0%}')

    # TODO: extra menu entries:
    # - backup everything
    # - continue but exclude ml dirs/files

    def edit_config_file(self, _):
        run(['open', '-a', 'TextEdit', CONFIG_PATH])

    def install_thrifty(self, _):
        if THRIFTY_PROXY.exists():
            rumps.alert("ThriftyBackup",
                        f"{THRIFTY_PROXY} already exists; not overwriting it.")
            return
        THRIFTY_PROXY.write_text(f"#!/bin/bash\n{THRIFTY} $@")
        THRIFTY_PROXY.chmod(0o744)
        rumps.alert("ThriftyBackup",
                    f"Installed 'thrifty' to {THRIFTY_PROXY.parent}")

    def edit_exclude_file(self, _, exclude_file, large_files_file=None):
        run(['open', '-a', 'TextEdit', exclude_file])
        if large_files_file:
            Popen(['qlmanage', '-p', large_files_file], stderr=DEVNULL)

    def show_files(self, _, ncdu_export_path):
        script = TERMINAL_NCDU.format(file=ncdu_export_path)
        run(['osascript', '-e', script])

    def abort_backup(self, _):
        self.daemon.abort_backup()
        self.idle()
        
    def quit(self, _):
        self.daemon.shutdown()
        rumps.quit_application()


APP_CONTENTS_PATH = Path(os.environ.get('RESOURCEPATH', __file__)).parent
THRIFTY = APP_CONTENTS_PATH / 'MacOS' / 'thrifty'
THRIFTY_PROXY = Path('/usr/local/bin') / THRIFTY.name

TERMINAL_NCDU = """
tell app "Terminal"
  do script "ncdu --color off --apparent-size -f {file}; exit"
  set current settings of first window to settings set "ocean"
  activate
end tell
"""


def app(echo=False, dry_run=False):
    if not CONFIG_PATH.exists():
        print(f"Creating configuration file at {CONFIG_PATH}")
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        CONFIG_PATH.write_text(CONFIG_TEMPLATE)
        rumps.alert("ThriftyBackup",
                    f"Created a sample configuration file at {CONFIG_PATH}."
                    " Now, please select 'Edit configuration file' from the"
                    " menu and add one or more backup configurations.")
    app = MenuBarApp(echo, dry_run)
    app.run()


if __name__ == "__main__":
    app()
