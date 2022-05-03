import datetime
import typing

from s3_backup import BackupItem
from s3_backup.backup_item import BackupItemWrapper


class SuffixedKey(BackupItem):
    def __init__(self, key: str, underlying: BackupItem):
        self._key = key
        self.underlying = underlying

    def key(self) -> str:
        return self._key

    def __repr__(self) -> str:
        return f"<SuffixedKey {self._key}: {self.underlying}>"

    def fileobj(self) -> typing.Generator[typing.BinaryIO, None, None]:
        return self.underlying.fileobj()

    def should_upload(self, modification_time: typing.Optional[datetime.datetime],
                      metadata: typing.Optional[typing.Mapping[str, str]]) -> BackupItem.ShouldUpload:
        return self.underlying.should_upload(
            modification_time=modification_time,
            metadata=metadata,
        )



class AddKeySuffixWrapper(BackupItemWrapper):
    def __init__(
            self,
            underlying_it: typing.Iterator[BackupItem],
            suffix: str,
    ):
        super().__init__(underlying_it)
        self.suffix = suffix

    def __iter__(self) -> typing.Generator[BackupItem, None, None]:
        for item in self.underlying_it:
            yield SuffixedKey(
                key=item.key() + self.suffix,
                underlying=item,
            )
