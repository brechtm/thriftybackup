from pathlib import Path


__version__ = '0.0.2'


CONFIG_DIR = Path.home() / '.config' / 'thriftybackup'
CONFIG_PATH = CONFIG_DIR / 'config.toml'

CACHE_DIR = Path.home() / '.cache' / 'thriftybackup'
