#!/bin/env python

import subprocess
import sys

from datetime import datetime, timedelta
from pathlib import Path

from pid.decorator import pidfile


PATH = Path(__file__).parent


ARGS = [
  "--links",
  "--exclude-from", "rclone_backup.exclude",
  "--bwlimit", "400k:off",
  "--log-level", "INFO",
]

def rclone(command, source, destination, *extra_args):
  timestamp = datetime.now().isoformat()
  logfile = f"{timestamp}_{command}.log"
  backupdir = f"{DESTINATION}_{timestamp}"
  cmd = ["caffeinate", "/usr/local/bin/rclone", command, "--log-file", logfile,
         "--backup-dir", backupdir, *ARGS, *extra_args, source, destination]
#   print(" ".join(cmd))
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
def main():
  _, *extra = sys.argv
  now = datetime.now()
  any_age = age_of_last_backup(now)
  sync_age = age_of_last_backup(now, "sync")

  if not sync_age or sync_age > timedelta(days=7):  # full sync every week
    rclone("sync", SOURCE, DESTINATION, "--checkers", "32", "--fast-list",
           "--track-renames", "--track-renames-strategy", "modtime,leaf", *extra)
  elif any_age > timedelta(hours=3):                # "top up" every 3 hours
    age_in_seconds = int(any_age.total_seconds()) + 60  # safety margin
    rclone("copy", SOURCE, DESTINATION, "--max-age", str(age_in_seconds),
           "--no-traverse", *extra)


if __name__ == "__main__":
  main()
