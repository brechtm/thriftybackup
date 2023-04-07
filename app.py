
import os
import time

from pathlib import Path
from queue import Queue
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
        config = Configuration(CONFIG_PATH, echo=echo, dry_run=dry_run)
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

    def add_show_files_menu_item(self):
        self.add_menuitem('Show Files', self.show_files, 'f')

    def idle(self):
        self.title = None
        self.menu.clear()
        self.add_menuitem('Edit configuration', self.edit_config_file, ',')
        self.add_menuitem('Install command-line tool', self.install_thrifty, 'c')
        self.add_menuitem('Quit', rumps.quit_application, 'q')
        self.backup_name = None
        self.exclude_file = None
        self.ncdu_export_path = None

    def prepare(self, backup_name):
        self.menu.clear()
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
        self.set_title(f"{backup_name}: {format_size(total_size)}",
                       color=(1, 0, 0, 1))
        self.add_menuitem('Continue Backup', self.continue_backup, 'c')
        self.add_menuitem('Skip Backup', self.skip_backup, 's')
        self.add_menuitem('Edit Exclude File', self.edit_exclude_file, 'x')
        self.add_show_files_menu_item()
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
        self.add_show_files_menu_item()
        self.add_menuitem('Abort Backup', self.abort_backup, 'a')
        self.set_title(format_size(total_bytes))

    def update_progress(self, transferred):
        self.progress_menu_item.title = \
            (f'{self.backup_name}: {format_size(transferred)}'
             f' of {format_size(self.total_bytes)}')
        self.set_title(f'{transferred / self.total_bytes:.0%}')

    # TODO: extra menu entries:
    # - backup everything
    # - continue but exclude ml dirs/files

    def skip_backup(self, _):
        self.quit()

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
