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


def common_prefix(a: str, b: str) -> str:
    return os.path.commonprefix([a, b])


def group_files(files: typing.Iterator[BackupItem], min_size: int) -> typing.Iterator[GroupedItem]:
    files = sorted(files, key=lambda item: item.key())

    def longest_entry_index(l: typing.Iterable[str]) -> int:
        max_len = 0
        max_len_i = None
        for i, item in enumerate(l):
            key_len = len(item)
            if key_len > max_len:
                max_len = key_len
                max_len_i = i
        return max_len_i

    def get_longest_group():
        # Find longest item in the list
        max_len_i = longest_entry_index((e.key() for e in files))

        matching_prefix = files[max_len_i].key()
        size = files[max_len_i].size()
        start = end = max_len_i
        while size < min_size:
            if start == 0 and end == len(files)-1:
                # got full list, break without meeting min_size requirement
                break

            # Try to extend our sublist to either above or below (whichever has the longest common prefix)
            if start > 0:
                common_prefix_before_start = len(common_prefix(files[start-1].key(), files[end].key()))
            else:
                common_prefix_before_start = -1
            if end < len(files)-1:
                common_prefix_after_end = len(common_prefix(files[start].key(), files[end+1].key()))
            else:
                common_prefix_after_end = -1

            if common_prefix_before_start >= common_prefix_after_end:
                matching_prefix_len = common_prefix_before_start
                matching_prefix = files[end].key()[0:matching_prefix_len]
                while start > 0 and \
                        files[start-1].key()[0:matching_prefix_len] == matching_prefix:
                    size += files[start-1].size()
                    start = start - 1
            if common_prefix_before_start <= common_prefix_after_end:
                matching_prefix_len = common_prefix_after_end
                matching_prefix = files[start].key()[0:matching_prefix_len]
                while end < len(files)-1 and \
                        files[end+1].key()[0:matching_prefix_len] == matching_prefix:
                    size += files[end+1].size()
                    end = end + 1

        g = GroupedItem(
            key=matching_prefix,
            underlying=files[start:(end+1)],
            size=size,  # TODO: remove
        )
        del files[start:(end+1)]
        return g

    while len(files):
        yield get_longest_group()


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
