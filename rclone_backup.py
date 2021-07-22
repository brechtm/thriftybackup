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


@pidfile(piddir=PATH)
def main():
  try:
    _, action, *extra = sys.argv
  except ValueError:
    raise SystemExit("ERROR: no action supplied (copy, sync)")

  logs = [logfile.name for logfile in PATH.glob("*.log")]
  last_log = sorted(logs)[-1]
  last_timestamp, _ = last_log.split("_")
  age = datetime.now() - datetime.fromisoformat(last_timestamp)
  if action == "copy":
    if age < timedelta(hours=3):  # skip backup if we recently took one
      return
    age_in_seconds = int(age.total_seconds()) + 60	# safety margin
    rclone("copy", SOURCE, DESTINATION, "--max-age", str(age_in_seconds),
           "--no-traverse", *extra)
  elif action == "sync":  # TODO: sync when last sync was over a week ago
    rclone("sync", SOURCE, DESTINATION, "--checkers", "32", "--fast-list",
           "--track-renames", "--track-renames-strategy", "modtime,leaf", *extra)
  else:
  	raise SystemExit(f"ERROR: unknown action: {action}")


if __name__ == "__main__":
  main()
