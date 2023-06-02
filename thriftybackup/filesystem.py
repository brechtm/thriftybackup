

class Entry:
    def __init__(self, path, size=None, action='copy', **metadata):
        self.path = path
        self.size = size
        self.action = action
        self.metadata = metadata

    def calculate_size(self):
        raise NotImplementedError

    def to_ncdu(self, name):
        raise NotImplementedError
        
        
class File(Entry):
    def calculate_size(self):
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

    def add_file(self, path, size, link=False, action='copy', **metadata):
        parts = path.parts
        self._add_file(path, parts, size, link=link, action=action, **metadata)

    def _add_file(self, path, path_parts, size, link=False, action=False,
                  **metadata):
        name, *parts = path_parts
        dir_path = '/'.join(path.parts[:-len(parts)]) + '/'
        if parts:
            dir = self.entries.setdefault(name, Directory(dir_path))
            dir._add_file(path, parts, size, link=link, action=action,
                          **metadata)
        else:
            assert name not in self.entries
            self.entries[name] = (Link if link else File)(path, size, action,
                                                          **metadata)
            

    def calculate_size(self):
        self.size = sum((e.calculate_size() for e in self.entries.values()),
                        start=0)
        return self.size

    def iter_files(self, exclude):
        if self not in exclude:
            for entry in self.entries.values():
                yield from entry.iter_files(exclude)

    def to_ncdu(self, name):
        return [dict(name=name),
                *(entry.to_ncdu(name) for name, entry in self.entries.items())]


def find_large_entries(entry, threshold):
    if entry.action != 'copy' or entry.size < threshold:
        return
    try:
        entries = entry.entries
    except AttributeError:  # entry is a file
        yield entry
    else:                   # entry is a directory
        yield_this_dir = True
        for name, child in entries.items():
            for entry in find_large_entries(child, threshold):
                yield entry
                yield_this_dir = False
        if yield_this_dir:
            yield entry
