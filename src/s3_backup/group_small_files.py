import dataclasses
import functools
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
        self.underlying_list = underlying
        self.size = size

    def key(self) -> str:
        return self._key

    def __repr__(self) -> str:
        return f"<GroupedItem {self._key}: {self.underlying_list}>"


class Tree:
    @dataclasses.dataclass
    class Element:
        key: str
        size: int
        item: BackupItem

        def __lt__(self, other):
            return self.key < other.key

    def __init__(self):
        self.elements: typing.List[Tree.Element] = []
        self.key_prefixes: typing.Dict[str, "Tree"] = {}

    def add_elements(self, elements: typing.Iterable[Element]):
        for el in elements:
            self.add_element(el)

    def add_element(self, element: Element):
        key = element.key
        if len(key) <= 1:
            self.elements.append(element)
        else:
            first_char = key[0]
            if first_char not in self.key_prefixes:
                self.key_prefixes[first_char] = Tree()
            self.key_prefixes[first_char].add_element(Tree.Element(
                key=element.key[1:],
                size=element.size,
                item=element.item,
            ))

    def elements_size(self) -> int:
        size = 0
        for element in self.elements:
            size += element.size
        return size

    def children_size(self) -> int:
        size = 0
        for node in self.key_prefixes.values():
            size += node.size()
        return size

    def size(self) -> int:
        return self.children_size() + self.elements_size()

    def flatten(self):
        for key_prefix in list(self.key_prefixes.keys()):
            node = self.key_prefixes[key_prefix]
            node.flatten()

            if len(node.key_prefixes) == 1 and len(node.elements) == 0:
                only_prefix = next(iter(node.key_prefixes.keys()))
                self.key_prefixes[key_prefix + only_prefix] = node.key_prefixes[only_prefix]
                del self.key_prefixes[key_prefix]

    def merge_min_size(self, min_size: int):
        for key_prefix in list(self.key_prefixes.keys()):
            node = self.key_prefixes[key_prefix]
            node.merge_min_size(min_size)

            if len(node.elements) and node.elements_size() < min_size:
                # Elements in the node are too small, pull them up to this level
                self.elements.extend((
                    Tree.Element(key=key_prefix + el.key, size=el.size, item=el.item)
                    for el in node.elements
                ))
                node.elements = []

            if len(node.elements) == 0 and len(node.key_prefixes) == 0:
                # Remove empty nodes
                del self.key_prefixes[key_prefix]


def group_files(items: typing.Iterator[BackupItem], min_size: int) -> typing.Iterator[GroupedItem]:
    tree = Tree()
    tree.add_elements((
        Tree.Element(item.key(), item.size(), item)
        for item in items
    ))
    tree.flatten()  # reduce recursion
    tree.merge_min_size(min_size)
    tree.flatten()  # again, since we pulled up elements in the merge_min_size()-step

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

    return recurse_tree(tree)


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
        self.suffix = '*~.zip'

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(size_threshold={self.size_threshold})"

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
        num = 0
        min_size = None
        max_size = None
        for entry in group_files(small_files, min_size=self.size_threshold):
            entry._key = entry._key + self.suffix
            logger.info(f"`{entry.key()}`  "
                        f"({len(entry.underlying_list)} entries, "
                        f"{humanize.naturalsize(entry.size, binary=True)})"
                        "\n    " + "\n    ".join([str(_) for _ in entry.underlying_list]))
            num += 1
            if min_size is None or entry.size < min_size:
                min_size = entry.size
            if max_size is None or entry.size > max_size:
                max_size = entry.size
        logger.info(f"Would have generated {num} ZIPs, ranging from "
                    f"{humanize.naturalsize(min_size, binary=True)} to "
                    f"{humanize.naturalsize(max_size, binary=True)}")
