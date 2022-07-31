#!/bin/env python

""" Perform incremental backups to the cloud using rclone

Usage:

1. Setup virtual environment

    $ cd /path/to/script
    $ python3 -m venv .venv
    $ .venv/bin/pip install -r requirements.txt

2. Add the following to your crontab:

    */15	*	*	*	*	/path/to/script/rclone_backup

    You should configure your OS to send you an email on a cron job failure.

3. In macOS, you need to grant Full Disk Access in Security & Privacy to:

    - /usr/sbin/cron
    - /usr/local/bin/rclone

   (see also https://apple.stackexchange.com/questions/375383)

"""

import argparse
import json
import re
import subprocess
import sys

from copy import copy
from datetime import datetime, timedelta
from pathlib import Path

from pid import PidFileError
from pid.decorator import pidfile


BACKUPS = [
  # source, destination, exclude file, top-up interval, full sync interval
  ("/Users/brechtm", "crypt:Backup/MacBook/Users/brechtm",
   "brechtm.exclude", timedelta(hours=6), timedelta(days=7)),
  ("/Users/brechtm/Library", "crypt:Backup/MacBook/Users/brechtm_Library",
   "brechtm_Library.exclude", timedelta(days=2), timedelta(days=7)),
]

PATH = Path(__file__).parent



class RcloneBackup:
  def __init__(self, source, destination, exclude_file, copy_interval,
               sync_interval, extra_args, dry_run=False, echo=False):
    self.source = Path(source)
    self.destination = Path(destination)
    self.exclude_file = exclude_file
    self.copy_interval = copy_interval
    self.sync_interval = sync_interval
    self.extra_args = extra_args
    self.dry_run = dry_run
    self.echo = echo

  @property
  def name(self):
    return self.destination.name

  @property
  def logs_path(self):
    return PATH / "logs" / self.name

  def rclone(self, *args, dry_run=None, input=None, capture=False):
    dry_run = self.dry_run if dry_run is None else dry_run
    cmd = ["/usr/local/bin/rclone", *args, *(self.extra_args if not capture
                                             else ())]
    if self.echo or dry_run:
      print(" ".join(map(str, cmd)))
    if not dry_run:
      return subprocess.run(cmd, cwd=PATH, input=input, capture_output=capture,
                            encoding="utf-8", check=True)

  BACKUP_ARGS = [
    "--links",
    "--local-no-check-updated",   # https://forum.rclone.org/t/transport-connection-broken/16494/4
    "--bwlimit", "400k:off",
    "--log-level", "INFO",
  ]

  RE_LOG_ERROR = re.compile(r"\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ERROR :")
  RE_LOG_STATUS = re.compile(r"^(?P<field>Transferred|Errors|Checks|Deleted|Renamed|Transferred|Elapsed time):")

  def backup(self, force=None):
    """Create an incremental backup from ``source`` to ``destination``

    Args:
      force (bool): perform a backup even if the copy or sync interval ...

    Returns:
      bool: ``True`` if a backup was performed, ``False`` if the interval ...

    """
    last_age, sync_age = self.get_last_backup_age()
    # full sync
    if force == "sync" or (force is None
                           and (not sync_age or sync_age > self.sync_interval)):
      cmd, args = "sync", ("--fast-list", "--retries", "1", "--track-renames",
                           "--track-renames-strategy", "modtime,leaf")
    # "top up" copy
    elif force == "copy" or last_age > self.copy_interval:
      max_age = int(last_age.total_seconds()) + 60  # safety margin
      cmd, args = "copy", ("--max-age", str(max_age), "--no-traverse")
    else:
      return False

    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M")
    last_log, last_timestamp = self.get_last_log()
    log_filename = f"{self.name}_{timestamp}_{cmd}.log"
    log_path = self.logs_path / log_filename
    backupdir = self.destination / last_timestamp
    try:
      self.rclone(cmd, "--log-file", log_path, "--backup-dir", backupdir,
                  "--exclude-from", self.exclude_file, *args, *self.BACKUP_ARGS,
                  *self.extra_args, self.source, self.destination / "latest")
    except subprocess.CalledProcessError as cpe:
      rc = cpe.returncode
      info = RCLONE_EXIT_CODES[rc]
      print(f"rclone returned non-zero exit status {rc} - {info}.\n"
            f"The log file is {log_filename}\n"
            f"These are the errors:\n")
      status = {}
      with log_path.open() as log:
        for line in log:
          if self.RE_LOG_ERROR.match(line):
            print(line, end='')
          else:
            m = self.RE_LOG_STATUS.match(line)
            if m:
              status[m.group('field')] = line
      for line in status.values():
        print(line, end='')
      raise SystemExit(rc)
    finally:
      self.record_backup_size(backupdir)
      self.rclone("move", self.destination / last_log, backupdir)
      self.rclone("copy", log_path, self.destination)
      # self.purge(destination)
    return True

  def combine(self, old, new):
    if self.echo:
      print(f"# combining {old.name} and {new.name} backups")
    log_path = self.logs_path / f"{self.name}_{old.name}_combine.log"
    self.rclone("move", new, old, "--delete-empty-src-dirs", "--log-file",
                log_path, "--log-level", "INFO")
    self.rclone("rmdir", new)
    self.rclone("move", old, new, "--delete-empty-src-dirs")
    self.rclone("copy", log_path, new)
    return new

  def purge(self):
    now = datetime.now()
    backups = self.list_files(self.destination, exclude="/latest/",
                              recursive=False, dirs_only=True)
    daily = None
    weekly = None
    combine_path = None
    combining = False
    for backup in backups:
      path = self.destination / backup
      timestamp = datetime.fromisoformat(backup)
      age = now - timestamp
      day = timestamp.date()
      month = day.year, day.month
      week = day.year, day.isocalendar().week

      if age < timedelta(days=7):       # keep all backups
        pass
      elif age < timedelta(days=31):    # keep daily backups
        if daily and daily == day:
          combining = True
          combine_path = self.combine(combine_path, path)
          continue
        daily = day
      else:                             # keep weekly backups
        if weekly and weekly == week:
          combining = True
          combine_path = self.combine(combine_path, path)
          continue
        weekly = week

      if combining:
        if self.echo:
          print(f'# calculate size for {combine_path}')
        self.rclone("delete", "--include", f"/{self.name}_*_size_*",
                    combine_path)
        self.record_backup_size(combine_path)
        combining = False

      if self.echo:
        print('# keeping ', timestamp)
      combine_path = path


  def list_files(self, path, include=None, exclude=None, recursive=True,
                 dirs_only=False, files_only=False):
    args = ((["--dirs-only"] if dirs_only else [])
            + (["--files-only"] if files_only else [])
            + (["--include", include] if include else [])
            + (["--exclude", exclude] if exclude else [])
            + (["--recursive"] if recursive else []))
    list_cmd = self.rclone("lsf", path, "--dir-slash=false", *args,
                           dry_run=False, capture=True)
    return sorted(list_cmd.stdout.split())

  def get_last_log(self):
    logs = self.list_files(self.destination, include=f"/{self.name}_*.log",
                           recursive=False, files_only=True)
    if not logs:  # this is the first backup
      return None, None
    try:
      last_log, = logs
    except ValueError:
      raise SystemExit(f"There should only be a single '{backup_dir}' log file"
                       f" in {self.destination}!")
    _, last_backup_timestamp, _ = last_log.rsplit("_", maxsplit=2)
    return last_log, last_backup_timestamp

  def get_last_backup_age(self):
    now = datetime.now()
    last_log, last_timestamp = self.get_last_log()
    try:
      last_age = now - datetime.fromisoformat(last_timestamp)
    except TypeError:
      return None, None
    sync_logs = self.list_files(self.destination,
                                include=f"/*/{self.name}_*_sync.log",
                                files_only=True)
    if sync_logs:
      _, last_sync_timestamp, _ = sorted(sync_logs)[-1].rsplit("_", maxsplit=2)
    else:
      last_sync_timestamp = None  # breaks when first backup is not a sync
    sync_age = (last_age if last_log.endswith("sync.log")
                else now - datetime.fromisoformat(last_sync_timestamp))
    return last_age, sync_age

  def record_backup_size(self, snapshot):
    size_cmd = self.rclone("size", "--json", "--exclude",
                           f"/{self.name}_*.log", snapshot, capture=True)
    size = 0 if self.dry_run else json.loads(size_cmd.stdout)["bytes"]
    size_filename = f"{self.name}_{snapshot.name}_size_{size}"
    self.rclone("touch", snapshot / size_filename)

  RE_SIZE = re.compile(r"^(.+)\/.*_\1_size_(\d+)$")

  def snapshot_sizes(self):
    parent_dir, name = self.destination.parent, self.destination.name
    size_files = self.list_files(self.destination,
                                 include=f"/*/{self.name}_*_size_*",
                                 files_only=True)
    total = 0
    for path in size_files:
      timestamp, size_str = self.RE_SIZE.match(path).groups()
      size = int(size_str)
      print(f"{timestamp:42} {human_size(size):>12}")
      total += size
    print(f"Total: {human_size(total):>12}")


# https://rclone.org/docs/#exit-code
RCLONE_EXIT_CODES = {
  1: "Syntax or usage error",
  2: "Error not otherwise categorised",
  3: "Directory not found",
  4: "File not found",
  5: "Temporary error (one that more retries might fix) (Retry errors)",
  6: "Less serious errors (like 461 errors from dropbox) (NoRetry errors)",
  7: "Fatal error (one that more retries won't fix, like account suspended) (Fatal errors)",
  8: "Transfer exceeded - limit set by --max-transfer reached",
  9: "Operation successful, but no files transferred",
}

# https://stackoverflow.com/a/59174649/438249
def human_size(size):
  for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
    if size < 1000.0:
      return "%3.1f %s" % (size, x)
    size /= 1000.0
  return size


@pidfile(piddir=PATH)
def main(force=None, echo=False, dry_run=False, extra=[]):
  for source, destination, exclude_file, copy_interval, sync_interval in BACKUPS:
    rclone_backup = RcloneBackup(source, destination, exclude_file,
                                 copy_interval, sync_interval, extra,
                                 dry_run=dry_run, echo=echo)
    if force == "purge":
      rclone_backup.purge()
    elif force == "size":
      rclone_backup.snapshot_sizes()
    elif rclone_backup.backup(force=force):
      break   # only continue to next backup if current one is skipped


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--echo", action="store_true",
                      help="Print the rclone command before executing")
  parser.add_argument("--force", choices=["sync", "copy", "purge", "size"],
                      help="Force a 'sync' or 'copy', regardless of when the "
                           "last backup was performed")
  parser.add_argument("--dry-run", action="store_true",
                      help="Do not execute the rclone command (implies --echo)")
  parser.add_argument('extra_args', nargs=argparse.REMAINDER,
                      metavar="-- <extra args>",
                      help="All arguments trailing '--'are passed on to rclone")
  args = parser.parse_args()
  args.echo = args.echo or args.dry_run
  extra = args.extra_args[1:] if args.extra_args else []
  try:
    main(args.force, args.echo, args.dry_run, extra)
  except PidFileError:
    pass
