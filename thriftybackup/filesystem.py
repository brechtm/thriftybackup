
import json

from functools import cached_property
from itertools import chain
from pathlib import Path


class Entry:
    def __init__(self, path, size=None, action=None, **metadata):
        self.path = path
        self.size = size
        self.action = action
        self.metadata = metadata

    @property
    def transfer_size(self):
        raise NotImplementedError

    def large_entries(self, threshold):
        if self.transfer_size > threshold:
            yield self

    def to_ncdu(self, name):
        raise NotImplementedError
        
        
class File(Entry):
    @property
    def transfer_size(self):
        return self.size if self.action == 'copy' else 0

    def iter_files(self, exclude):
        if self not in exclude:
            yield self

    def to_ncdu(self, name):
        extra = dict(excluded=self.action) if self.action != 'copy' else {}
        match self.action:
            case 'delete': name += ' [D]'
            case 'move': name += f' [M > {self.metadata["destination"]}]'
            case 'move-dest': name += f' [M < {self.metadata["source"]}]'
        return dict(name=name, asize=self.size, **extra)


class Link(File):
    def to_ncdu(self, name):
        return dict(name=name, notreg=True)


class Directory(Entry):
    def __init__(self, path):
        super().__init__(path)
        self.entries = {}

    def get(self, path: str) -> Entry:
        return self._get(path.split('/'))

    def _get(self, parts):
        name, *rest = parts
        return self.entries[name]._get(rest) if rest else self.entries[name]

    def add_file(self, path, size, action, **metadata):
        path = Path(path)
        link = path.name.endswith('.rclonelink')
        return self._add_file(path, path.parts, size, action, link=link,
                              **metadata)

    def _add_file(self, path, path_parts, size, action, link=False, **metadata):
        name, *parts = path_parts
        dir_path = Path(*path.parts[:-len(parts)])
        if parts:
            dir = self.entries.setdefault(name, Directory(dir_path))
            dir._add_file(path, parts, size, action, link=link, **metadata)
        else:
            if existing_entry := self.entries.get(name):
                if existing_entry.action:
                    raise RuntimeError(f"{path} has already been added")
                existing_entry.action = action
            else:
                entry = (Link if link else File)(path, size, action, **metadata)
                self.entries[name] = entry

    @cached_property
    def transfer_size(self):
        return sum((e.transfer_size for e in self.entries.values()), start=0)

    def large_entries(self, threshold):
        large_children = chain(*(entry.large_entries(threshold)
                                 for entry in self.entries.values()))
        try:
            yield next(large_children)
            yield from large_children
        except StopIteration:   # large_children is empty
            yield from super().large_entries(threshold)

    def iter_files(self, exclude):
        if self not in exclude:
            if self.action:
                yield self
            for entry in self.entries.values():
                yield from entry.iter_files(exclude)

    def to_ncdu(self, name):
        return [dict(name=name),
                *(entry.to_ncdu(name) for name, entry in self.entries.items())]


class Root(Directory):
    def __init__(self, source_path):
        super().__init__('')
        self.source_path = source_path
    
    def write_ncdu_export(self, ncdu_export_path):
        ncdu = [1, 2, dict(progname='thriftybackup', progver='0.0.0', timestamp=0),
                self.to_ncdu(str(self.source_path))]
        with ncdu_export_path.open('w') as f:
            json.dump(ncdu, f)
