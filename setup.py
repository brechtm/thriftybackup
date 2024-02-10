from setuptools import setup

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
    version='0.0.2',
    app=['app.py'],
    data_files=['rclone.icns'],
    options={'py2app': OPTIONS},
    setup_requires=['py2app'],
)
