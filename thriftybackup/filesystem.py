

from functools import cached_property
from itertools import chain
from pathlib import Path


class Entry:
    def __init__(self, path, size=None, action='copy', **metadata):
        self.path = path
        self.size = size
        self.action = action
        self.metadata = metadata

    @property
    def transfer_size(self):
        raise NotImplementedError

    def large_entries(self, threshold):
        if self.action == 'copy' and self.size > threshold:
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

    def add_file(self, path, size, action='copy', **metadata):
        path = Path(path)
        link = path.name.endswith('.rclonelink')
        return self._add_file(path, path.parts, size, link=link, action=action,
                              **metadata)

    def _add_file(self, path, path_parts, size, link=False, action=False,
                  **metadata):
        name, *parts = path_parts
        dir_path = '/'.join(path.parts[:-len(parts)]) + '/'
        if parts:
            dir = self.entries.setdefault(name, Directory(dir_path))
            dir._add_file(path, parts, size, link=link, action=action,
                          **metadata)
        else:
            if name in self.entries:
                raise RuntimeError(f"{path} has already been added")
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
            for entry in self.entries.values():
                yield from entry.iter_files(exclude)

    def to_ncdu(self, name):
        return [dict(name=name),
                *(entry.to_ncdu(name) for name, entry in self.entries.items())]
