#!/bin/env python

import argparse
import subprocess
import sys

from datetime import datetime, timedelta
from pathlib import Path

from pid.decorator import pidfile


ARGS = [
  "--links",
  "--exclude-from", "rclone_backup.exclude",
  "--bwlimit", "400k:off",
  "--log-level", "INFO",
]

PATH = Path(__file__).parent


def rclone(command, source, destination, *extra_args, echo=False):
  timestamp = datetime.now().isoformat()
  logfile = f"{timestamp}_{command}.log"
  backupdir = f"{DESTINATION}_{timestamp}"
  cmd = ["caffeinate", "/usr/local/bin/rclone", command, "--log-file", logfile,
         "--backup-dir", backupdir, *ARGS, *extra_args, source, destination]
  if echo:
    print(" ".join(cmd))
  subprocess.run(cmd, cwd=PATH, check=True)

SOURCE = "/Users/brechtm/Documents"
DESTINATION = "crypt:Backup/MacBook/Users/brechtm/Documents"


def age_of_last_backup(now, backup_type=""):
  logs = [logfile.name for logfile in PATH.glob(f"*{backup_type}.log")]
  if not logs:
    return
  last_log = sorted(logs)[-1]
  last_timestamp, _ = last_log.split("_")
  return now - datetime.fromisoformat(last_timestamp)


@pidfile(piddir=PATH)
def main(force=None, echo=False, extra=[]):
  now = datetime.now()
  any_age = age_of_last_backup(now)
  sync_age = age_of_last_backup(now, "sync")

  # full sync every week
  if force == "sync" or not sync_age or sync_age > timedelta(days=7):
    rclone("sync", SOURCE, DESTINATION, "--fast-list", "--retries", "1",
           "--track-renames", "--track-renames-strategy", "modtime,leaf",
           *extra, echo=echo)
  # "top up" every 3 hours
  elif force == "copy" or any_age > timedelta(hours=3):
    age_in_seconds = int(any_age.total_seconds()) + 60  # safety margin
    rclone("copy", SOURCE, DESTINATION, "--max-age", str(age_in_seconds),
           *extra, echo=echo)


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--echo", action="store_true",
                      help="Print the rclone command before executing")
  parser.add_argument("--force", choices=["sync", "copy"],
                      help="Force a 'sync' or 'copy', regardless of when the "
                           "last backup was performed")
  parser.add_argument('extra_args', nargs=argparse.REMAINDER,
                      metavar="-- <extra args>",
                      help="All arguments trailing '--'are passed on to rclone")
  args = parser.parse_args()
  extra = args.extra_args[1:] if args.extra_args else []
  main(args.force, args.echo, extra)
