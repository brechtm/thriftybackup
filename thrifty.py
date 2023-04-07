
import argparse

from thriftybackup import CONFIG_PATH
from thriftybackup.config import Configuration


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--echo', action='store_true',
                        help="Echo the rclone commands before executing them")
    parser.add_argument('--dry-run', action='store_true',
                        help="Dry-run rclone commands")
    subparsers = parser.add_subparsers(dest='command', )#help='')
    parser_list = subparsers.add_parser('list', help='list backup snapshots')
    parser_list.add_argument('backup', help="The backup configuration for which"
                                            " to list snapshots")
    args = parser.parse_args()

    match args.command:
        case 'list':
            config = Configuration(CONFIG_PATH, echo=args.echo,
                                   dry_run=args.dry_run)
            if args.backup not in config:
                msg = (f"There is no backup named '{args.backup}'. Choose one"
                       f" from:\n" + '\n'.join(f'- {n}' for n in config))
                raise SystemExit(msg)
            backup = config[args.backup]
            backup.print_snapshot_sizes()
        case _:
            parser.print_help()


if __name__ == "__main__":
    main()
