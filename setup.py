from setuptools import setup

OPTIONS = {
    'argv_emulation': False,
    'argv_inject': ['app'],
    'plist': {
        'LSUIElement': True,
    },
    'packages': ['pid', 'rumps'],
}

setup(
    name='ThriftyBackup',
    app=['app.py'],
    data_files=['rclone.icns'],
    options={'py2app': OPTIONS},
    setup_requires=['py2app'],
)
