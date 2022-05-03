import re
import typing


from s3_backup.backup_item import BackupItemWrapper, BackupItem


class ExcludeReWrapper(BackupItemWrapper):
    def __init__(
            self,
            underlying_it: typing.Iterator[BackupItem],
            regex: str
    ):
        super().__init__(underlying_it)
        self.exclude_re = re.compile(regex)
        self.excluded_files = 0

    def summary(self) -> str:
        return f"{self.excluded_files} files excluded"

    def __iter__(self) -> typing.Generator[BackupItem, None, None]:
        for entry in self.underlying_it:
            if self.exclude_re.fullmatch(entry.key()):
                self.excluded_files += 1
            else:
                yield entry
