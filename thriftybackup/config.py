import re
import tomllib

from datetime import timedelta
from pathlib import Path

from .backup import RCloneBackup
from .util import EXPONENTS


class Configuration(dict):
    def __init__(self, config_path, echo, progress, dry_run) -> None:
        self.config_path = config_path
        self.echo = echo
        self.progress = progress
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
        value = self.toml.get(attribute, default)
        keep_match = RE_KEEP.fullmatch(value.strip())
        try:
            return int(keep_match.group('days'))
        except AttributeError:
            self._syntax_error(attribute)

    def _parse_interval(self, name, config):
        try:
            interval_match = RE_INTERVAL.fullmatch(config['interval'].strip())
        except KeyError:
            return None
        try:
            return timedelta(**{key: int(value) for key, value in
                                interval_match.groupdict(0).items()})
        except AttributeError:
            self._syntax_error('interval', section=name)

    def _create_backup(self, name, cfg):
        src, dest = cfg['source'], cfg['destination']
        interval = self._parse_interval(name, cfg)
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
                            bwlimit=self.bwlimit, rclone=self.rclone,
                            echo=self.echo, progress=self.progress,
                            dry_run=self.dry_run)


RE_KEEP = re.compile(r'(?P<days>\d+)\s*(d(ays?)?)?', re.IGNORECASE)

RE_INTERVAL = re.compile(r'((?P<days>\d+?)\s*(d|days?))?\s*'
                         r'((?P<hours>\d+?)\s*(h|hours?))?\s*'
                         r'((?P<minutes>\d+?)\s*(m|minutes?))?', re.IGNORECASE)

RE_THRESHOLD = re.compile(r'(?P<number>\d+)\s*(?P<unit>[KMGT])B?',
                          re.IGNORECASE)


CONFIG_TEMPLATE = f"""\
## General settings
## (uncomment and adjust)

# rclone = "/usr/local/bin/rclone"
# bwlimit = "400K"
# keep_all = "7 days"
# keep_daily = "31 days"

## Backup configurations

# [home]
## passes ~/.config/thriftybackup/home.exclude as exclude file to rclone
# source = "{Path.home()}"
# destination = "backup:home"
# interval = "4 hours"
# threshold = "100 MB"

# [Photos]
## passes ~/.config/thriftybackup/Photos.exclude as exclude file to rclone
# source = "/Volumes/Photos"
# destination = "backup:Photos"
## no interval set; backups need to be started manually through the menu
# threshold = "1 GB"
"""
