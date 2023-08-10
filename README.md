
# ThriftyBackup

ThriftyBackup is a simple incremental cloud backup application for macOS.
Because it makes use of [rclone](https://rclone.org/) under the hood, a wide
variety of cloud storage services are supported.

The distinctive feature of ThriftyBackup is that it notifies you when the backup
size exceeds a user-configurable threshold. At this point, you can select which
of the large files/directories should be included in the backup. This way, you
can prevent unnecessarily backing up large files, saving bandwidth and cloud
storage space.

> **:warning: Warning:** ThriftyBackup is currently alpha-quality software. Do
not rely on it as your (only) backup solution!
>
> Unfortunately, I also lack the time to develop it into a proper application,
but anyone interested is welcome to fork the project and give it a better
future.

Some of the other features are:
- (Reverse) incremental backups using rclone's `--backup-dir` argument
  - this creates copies of files that are updated/removed during an incremental
    backup
- Basic menu bar app, which
  - shows progress for the backup being performed
  - allows selecting the files to include in a backup exceeding the threshold
  - lists backup configurations and information on recent backups
  - can start [ncdu](https://dev.yorhel.nl/ncdu) so you can easily browse which
    files are to be backed up (if the threshold is exceeded), or have been
    backed up (for recent backups)
  - provides easy access to the configuration and exclude files
- Command line tool to list backup snapshots and their sizes
  - planned: pruning of backups (merges reverse snapshots)

## Backup configurations

ThiftyBackup supports multiple backup configurations, allowing for setting
individual backup intervals and size thresholds for different sets of file. For
each configuration, you specify:
- source: the directory to back up (this can be an external drive)
- destination: where on the cloud storage service to store backups for this
  backup source
- (optional) backup interval: if not provided, you need to trigger a backup
  manually
- (optional) size threshold: if not provided, will create a backup regardless of
  its size

You can also provide an [exclude file](https://rclone.org/filtering/#exclude-from-read-exclude-patterns-from-file)
for each configuration to limit which files and directories will be never be
backed up.


## Getting started

ThriftyBackup builds on [rclone](https://rclone.org) and APFS snapshots, so
these need to be set up before you can run it:
- APFS snapshots require Time Machine to be configured for your backup source
  - add a destination for Time Machine backups ('+' in System Settings >
    General > Time Machine)
    - this can be a tiny USB thumb drive that you won't actually use for backups
  - if you want to back up external drives, remove the volume from the TM
    exclusion list (Options...) (and add all contents to the exclusion list if
    you do not want to include the drive in regular TM backups)
- install rclone and [configure](https://rclone.org/commands/rclone_config/)
  it for your cloud storage service
- (optional) install [ncdu](https://dev.yorhel.nl/ncdu)

You also need to grant ThriftyBackup Full Disk Access in _System Settings >
Privacy & Security_.

After all this, you can start ThriftyBackup. On first run, it will create a
configuration file which you can open for editing through the ThriftyBackup
menu bar app menu. The configuration file includes some further hints.
