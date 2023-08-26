import dataclasses
import logging
import os.path
import typing

import humanize

from s3_backup import BackupItem
from s3_backup.backup_item import BackupItemWrapper


logger = logging.getLogger(__name__)


class GroupedItem():
    def __init__(self, key: str, underlying: typing.List[BackupItem], size: int = None):
        self._key = key
        self.underlying = underlying
        self.size = size

    def key(self) -> str:
        return self._key

    def __repr__(self) -> str:
        return f"<GroupedItem {self._key}: {self.underlying}>"


class Tree:
    @dataclasses.dataclass
    class Element:
        key: str
        size: int
        item: BackupItem

        def __lt__(self, other):
            return self.key < other.key

    def __init__(self, elements: typing.Iterable[BackupItem]):
        self.elements = sorted((
            Tree.Element(e.key(), e.size(), e)
            for e in elements
        ))
        self.key_prefixes = {}  # children

    def _split_off_children(self, min_size: int):
        size = {}
        char_start = 0
        previous_first_char = ''
        i = 0
        while i < len(self.elements):
            try:
                first_char = self.elements[i].key[0]
            except IndexError:
                first_char = ''

            if first_char != previous_first_char:
                if previous_first_char != '' and \
                        size[previous_first_char] > min_size:
                    self._extract_child(slice(char_start, i), previous_first_char)
                    i = char_start

                previous_first_char = first_char
                char_start = i

            if first_char not in size:
                size[first_char] = 0
            size[first_char] += self.elements[i].size

            i += 1

        if size[previous_first_char] > min_size:
            self._extract_child(slice(char_start, i), previous_first_char)

        for prefix in list(self.key_prefixes.keys()):
            child = self.key_prefixes[prefix]
            child._split_off_children(min_size)
            if len(child.elements) == 0 and len(child.key_prefixes) == 1:
                # Pull 1 level up
                only_prefix = next(iter(child.key_prefixes.keys()))
                self.key_prefixes[prefix + only_prefix] = child.key_prefixes[only_prefix]
                del self.key_prefixes[prefix]

    def _extract_child(self, s: slice, prefix: str):
        child = Tree([])  # Don't use constructor to avoid sorting again
        prefix_len = len(prefix)
        for el in self.elements[s]:
            child.elements.append(Tree.Element(el.key[prefix_len:], el.size, el.item))
        del self.elements[s]
        self.key_prefixes[prefix] = child

    def elements_size(self) -> int:
        size = 0
        for el in self.elements:
            size += el.size
        return size


def group_files(items: typing.Iterator[BackupItem], min_size: int) -> typing.Iterator[GroupedItem]:
    items = Tree(items)
    items._split_off_children(min_size)

    def recurse_tree(node: Tree, prefix: str = ''):
        for child_prefix, child in node.key_prefixes.items():
            for item in recurse_tree(child, prefix + child_prefix):
                yield item

        if len(node.elements) > 0:
            yield GroupedItem(
                key=prefix,
                underlying=[el.item for el in node.elements],
                size=node.elements_size(),
            )

    return recurse_tree(items)


class GroupSmallFilesWrapper(BackupItemWrapper):
    def __init__(
            self,
            underlying_it: typing.Iterator[BackupItem],
            size_threshold: int,
    ):
        super().__init__(underlying_it)
        self.size_threshold = size_threshold
        self.small_files = 0
        self.small_files_bytes = 0
        self.large_files = 0
        self.large_files_bytes = 0

    def summary(self) -> str:
        return f"{self.small_files} files, {humanize.naturalsize(self.small_files_bytes, binary=True)} " \
               f"< {humanize.naturalsize(self.size_threshold, binary=True)}\n" \
               f"{self.large_files} files, {humanize.naturalsize(self.large_files_bytes, binary=True)} " \
               f">= {humanize.naturalsize(self.size_threshold, binary=True)}"

    def __iter__(self) -> typing.Generator[BackupItem, None, None]:
        small_files = []
        for entry in self.underlying_it:
            size = entry.size()
            if size is not None and size < self.size_threshold:
                self.small_files += 1
                self.small_files_bytes += size
                small_files.append(entry)
            else:
                self.large_files += 1
                self.large_files_bytes += size
                yield entry

        for entry in small_files:
            yield entry

        logger.info("Would group:")
        for entry in group_files(small_files, min_size=self.size_threshold):
            logger.info(f"{entry.key()}  "
                        f"({humanize.naturalsize(entry.size, binary=True)})\n    "
                        + "\n    ".join([str(_) for _ in entry.underlying]))
