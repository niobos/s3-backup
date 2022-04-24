import typing

import humanize

from s3_backup import BackupItem
from s3_backup.backup_item import BackupItemWrapper


class GroupSmallFilesWrapper(BackupItemWrapper):
    def __init__(
            self,
            underlying_it: typing.Iterator[BackupItem],
            size_threshould: int,
    ):
        super().__init__(underlying_it)
        self.size_threshold = size_threshould
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
        for entry in self.underlying_it:
            size = entry.size()
            if size is not None:
                if size < self.size_threshold:
                    self.small_files += 1
                    self.small_files_bytes += size
                else:
                    self.large_files += 1
                    self.large_files_bytes += size

            yield entry
