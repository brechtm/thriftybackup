from setuptools import setup

from thriftybackup import __version__

OPTIONS = {
    'argv_inject': [
        '--echo',
        '--progress',
    ],
    'argv_emulation': False,
    'plist': {
        'LSUIElement': True,
    },
    'packages': ['rumps', 'watchdog'],
    'extra_scripts': ['thrifty.py'],
}

setup(
    name='ThriftyBackup',
    version=__version__,
    app=['app.py'],
    data_files=['rclone.icns'],
    options={'py2app': OPTIONS},
    setup_requires=['py2app'],
)
