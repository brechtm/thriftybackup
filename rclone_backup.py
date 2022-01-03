#!/bin/env python

""" Perform incremental backups to the cloud using rclone

Usage:

1. Setup virtual environment

    $ cd /path/to/script
    $ python3 -m venv .venv
    $ .venv/bin/pip install -r requirements.txt

2. Add the following to your crontab:

    */15	*	*	*	*	/path/to/script/rclone_backup

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

from datetime import datetime, timedelta
from pathlib import Path

from pid import PidFileError
from pid.decorator import pidfile


SOURCE = Path("/Users/brechtm")
DESTINATION = Path("crypt:Backup/MacBook/Users/brechtm")

ARGS = [
  "--links",
  "--local-no-check-updated",   # https://forum.rclone.org/t/transport-connection-broken/16494/4
  "--exclude-from", "rclone_backup.exclude",
  "--bwlimit", "400k:off",
  "--log-level", "INFO",
]

PATH = Path(__file__).parent
LOGS = PATH / "logs" / DESTINATION.name




def rclone(*args, dry_run=False, echo=False, input=None, capture=False):
  cmd = ["caffeinate", "/usr/local/bin/rclone", *args]
  if echo or dry_run:
    print(" ".join(map(str, cmd)))
  if not dry_run:
    return subprocess.run(cmd, cwd=PATH, input=input, capture_output=capture,
                          encoding="utf-8", check=True)


def backup(command, source, destination, *extra_args, max_age=None,
           traverse=False, dry_run=False, echo=False):
  timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M")
  last_log, last_timestamp = get_last_log(destination, echo=False)
  logfile = LOGS / f"{destination.name}_{timestamp}_{command}.log"
  backupdir = f"{destination}_{last_timestamp}"
  copy_args = (["--max-age", str(max_age)] if max_age else []
               + [] if traverse else ["--no-traverse"])
  try:
    rclone(command, "--log-file", logfile, "--backup-dir", backupdir,
           *ARGS, *copy_args, *extra_args, source, destination, dry_run=dry_run,
           echo=echo)
  except subprocess.CalledProcessError as cpe:
    rc = cpe.returncode
    print(f"rclone returned non-zero exit status {rc}. These are the errors:\n")
    with logfile.open() as log:
      for line in log:
        if RE_LOG.match(line):
          print(line, end='')
    raise SystemExit(rc)
  finally:
    record_backup_size(backupdir, dry_run=dry_run, echo=echo)
    rclone("move", f"{destination.parent}/{last_log}", backupdir, *extra_args,
           dry_run=dry_run, echo=echo)
    rclone("copy", logfile, destination.parent, *extra_args, dry_run=dry_run,
           echo=echo)
  purge(destination, *extra, dry_run=dry_run, echo=echo)


RE_LOG = re.compile(r"\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ERROR :"
                    r"|^(Transferred|Errors|Checks|Deleted|Renamed|Transferred|Elapsed time):")


def combine(old, new, *extra_args, dry_run=False, echo=False):
  _, old_dir = old.rsplit("/", maxsplit=1)
  _, new_dir = new.rsplit("/", maxsplit=1)
  if echo:
    print(f"# combining {old_dir} and {new_dir} backups")
  logfile = LOGS / f"{old_dir}_combine.log"
  rclone("move", new, old, "--delete-empty-src-dirs", "--log-file", logfile,
         "--log-level", "INFO", *extra_args, dry_run=dry_run, echo=echo)
  rclone("rmdir", new, *extra_args, dry_run=dry_run, echo=echo)
  rclone("move", old, new, "--delete-empty-src-dirs", *extra_args,
         dry_run=dry_run, echo=echo)
  rclone("copy", logfile, new, *extra_args, dry_run=dry_run, echo=echo)
  return new


def purge(destination, *extra_args, dry_run=False, echo=False):
  now = datetime.now()
  parent_dir, backup_dir = destination.parent, destination.name
  list_backups = rclone("lsf", "--dirs-only", "--dir-slash=false", "--include",
                        f"{backup_dir}_*/", parent_dir, echo=echo, capture=True)
  backups = sorted(list_backups.stdout.split())
  offset = len(backup_dir) + 1
  daily = None
  weekly = None
  combine_path = None
  combining = False
  for backup in backups:
    path = f"{parent_dir}/{backup}"
    timestamp = datetime.fromisoformat(backup[offset:])
    age = now - timestamp
    day = timestamp.date()
    month = day.year, day.month
    week = day.year, day.isocalendar().week

    if age < timedelta(days=7):       # keep all backups
      pass
    elif age < timedelta(days=31):    # keep daily backups
      if daily and daily == day:
        combining = True
        combine_path = combine(combine_path, path, *extra_args, dry_run=dry_run,
                               echo=echo)
        continue
      daily = day
    else:                             # keep weekly backups
      if weekly and weekly == week:
        combining = True
        combine_path = combine(combine_path, path, *extra_args, dry_run=dry_run,
                               echo=echo)
        continue
      weekly = week

    if combining:
      if echo:
        print(f'# calculate size for {combine_path}')
      _, combine_dir = combine_path.rsplit("/", maxsplit=1)
      rclone("delete", "--include", f"/{combine_dir}_size_*", combine_path,
             *extra_args, dry_run=dry_run, echo=echo)
      record_backup_size(combine_path, dry_run=dry_run, echo=echo)
      combining = False

    if echo:
      print('# keeping ', timestamp)
    combine_path = path


def list_files(path, pattern, dirs_only=False, files_only=False, echo=False):
  args = ((["--dirs-only"] if dirs_only else [])
          + (["--files-only"] if files_only else []))
  list_cmd = rclone("lsf", "--recursive", "--include", pattern, path,
                    *args, echo=echo, capture=True)
  return sorted(list_cmd.stdout.split())


def get_last_log(destination, echo=False):
  parent_dir, backup_dir = destination.parent, destination.name
  logs = list_files(parent_dir, f"/{backup_dir}_*.log", files_only=True,
                    echo=echo)
  if not logs:  # this is the first backup
    return None, None
  try:
    last_log, = logs
  except ValueError:
    raise SystemExit(f"There should only be a single '{backup_dir}' log file in"
                     f" {parent_dir}!")
  _, last_backup_timestamp, _ = last_log.split("_")
  return last_log, last_backup_timestamp


def get_last_backup_age(destination, echo=False):
  now = datetime.now()
  parent_dir, backup_dir = destination.parent, destination.name
  last_log, last_timestamp = get_last_log(DESTINATION, echo=echo)
  last_age = now - datetime.fromisoformat(last_timestamp)
  sync_logs = list_files(parent_dir, f"/{backup_dir}_*/{backup_dir}_*_sync.log",
                         files_only=True, echo=echo)
  if sync_logs:
    _, last_sync_timestamp, _ = sorted(sync_logs)[-1].rsplit("_", maxsplit=2)
  else:
    last_sync_timestamp = None  # breaks when first backup is not a sync
  sync_age = (last_age if last_log.endswith("sync.log")
              else now - datetime.fromisoformat(last_sync_timestamp))
  return last_age, sync_age


def record_backup_size(backupdir, dry_run=False, echo=False):
  backupdir = Path(backupdir)
  backup_name = backupdir.name
  size_cmd = rclone("size", "--json", "--exclude", f"/{backup_name}_*.log",
                    backupdir, dry_run=dry_run, echo=echo, capture=True)
  size = 0 if dry_run else json.loads(size_cmd.stdout)["bytes"]
  size_filename = backup_name + f"_size_{size}"
  rclone("touch", backupdir / size_filename, dry_run=dry_run, echo=echo)


@pidfile(piddir=PATH)
def main(force=None, echo=False, dry_run=False, extra=[]):
  last_age, sync_age = get_last_backup_age(DESTINATION)

  if force == "purge":
    purge(DESTINATION, *extra, dry_run=dry_run, echo=echo)
  # full sync every week
  elif force == "sync" or (force is None
                           and (not sync_age or sync_age > timedelta(days=7))):
    backup("sync", SOURCE, DESTINATION, "--fast-list", "--retries", "1",
           "--track-renames", "--track-renames-strategy", "modtime,leaf",
           *extra, dry_run=dry_run, echo=echo)
  # "top up" every 6 hours
  elif force == "copy" or last_age > timedelta(hours=6):
    age_in_seconds = int(last_age.total_seconds()) + 60  # safety margin
    backup("copy", SOURCE, DESTINATION, *extra, max_age=age_in_seconds,
           traverse=False, dry_run=dry_run, echo=echo)


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--echo", action="store_true",
                      help="Print the rclone command before executing")
  parser.add_argument("--force", choices=["sync", "copy", "purge"],
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
