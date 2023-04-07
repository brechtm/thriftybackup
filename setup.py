from setuptools import setup

OPTIONS = {
    'argv_emulation': False,
    'plist': {
        'LSUIElement': True,
    },
    'packages': ['rumps'],
    'extra_scripts': ['thrifty.py'],
}

setup(
    name='ThriftyBackup',
    app=['app.py'],
    data_files=['rclone.icns'],
    options={'py2app': OPTIONS},
    setup_requires=['py2app'],
)
