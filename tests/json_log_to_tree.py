from pathlib import Path

from thriftybackup.backup import scout_log_to_tree, sync_log_to_tree


source_path = '/Users/brechtm'
scout_log = '/Users/brechtm/.cache/thriftybackup/brechtm/brechtm_2023-07-04-212222_scout.log'
sync_log = scout_log.replace('_scout.log', '_sync.log')


with open(scout_log, 'rb') as l:
    tree = scout_log_to_tree(source_path, l)


for le in tree.large_entries(1024*1024):
    print(le.transfer_size, le.path)



# print(tree.get())
# raise SystemExit


excluded_paths = [
    # '.config',
    'Code/thriftybackup/.git/objects',
    # 'Code/Spoons',
    'Documents',
    'Pictures',
    # 'Tools/rclone',
]
excluded = [tree.get(path) for path in excluded_paths]


for entry in tree.iter_files(excluded):
    print(entry.path)



with open(sync_log, 'rb') as l:
    it = sync_log_to_tree(tree, l, dry_run=False)
    sync_tree = next(it)
    transferred = 0
    for item in it:
        print(item.size, item.path)
        transferred += item.size
    print(transferred)
    
sync_json_path = Path(Path(sync_log).with_suffix('.json').name)
sync_tree.write_ncdu_export(sync_json_path)
