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
import re
import subprocess
import sys

from datetime import datetime, timedelta
from pathlib import Path

from pid import PidFileError
from pid.decorator import pidfile


ARGS = [
  "--links",
  "--local-no-check-updated",   # https://forum.rclone.org/t/transport-connection-broken/16494/4
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
  try:
    subprocess.run(cmd, cwd=PATH, check=True)
  except subprocess.CalledProcessError as cpe:
    rc = cpe.returncode
    print(f"rclone returned non-zero exit status {rc}. These are the errors:\n")
    with (PATH / logfile).open() as log:
      for line in log:
        if RE_LOG.match(line):
          print(line, end='')
    raise SystemExit(rc)    
    

RE_LOG = re.compile(r"\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ERROR :"
                    r"|^(Transferred|Errors|Checks|Deleted|Renamed|Transferred|Elapsed time):")

SOURCE = "/Users/brechtm"
DESTINATION = "crypt:Backup/MacBook/Users/brechtm"


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
  # "top up" every 6 hours
  elif force == "copy" or any_age > timedelta(hours=6):
    age_in_seconds = int(any_age.total_seconds()) + 60  # safety margin
    rclone("copy", SOURCE, DESTINATION, "--max-age", str(age_in_seconds),
           "--no-traverse", *extra, echo=echo)


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
  try:
    main(args.force, args.echo, extra)
  except PidFileError:
    pass
