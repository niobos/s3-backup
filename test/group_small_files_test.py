import contextlib
import datetime
import typing

from s3_backup import BackupItem
from s3_backup.group_small_files import group_files


class MockItem(BackupItem):
    def __init__(self, key: str, size: int):
        self._key = key
        self._size = size

    def __repr__(self):
        return f"<Mock {self._key} {self._size}>"

    def key(self) -> str:
        return self._key

    def size(self) -> typing.Optional[int]:
        return self._size

    @contextlib.contextmanager
    def fileobj(self) -> typing.Generator[typing.BinaryIO, None, None]:
        yield None

    def should_upload(
            self,
            modification_time: typing.Optional[datetime.datetime],
            metadata: typing.Optional[typing.Mapping[str, str]],
    ) -> BackupItem.ShouldUpload:
        return BackupItem.ShouldUpload.DoUpload


def test_grouping_1():
    grouped = list(group_files([
        #group[1]:
        MockItem("foo", 1),

        # group[0]:
        MockItem("foo2/abc", 1),
        MockItem("foo2/def", 1),
        MockItem("foo2/ghi", 1),
    ], 2))
    assert len(grouped) == 2
    assert len(grouped[0].underlying_list) == 3
    assert len(grouped[1].underlying_list) == 1


def test_grouping_2():
    grouped = list(group_files([
        #group[1]:
        MockItem("foo", 1),

        # group[0]:
        MockItem(".git/a/b/c", 4),
        MockItem(".git/a/b/f", 4),
        MockItem(".git/c/d/e", 4),
    ], 10))
    assert len(grouped) == 2
    assert len(grouped[0].underlying_list) == 3
    assert len(grouped[1].underlying_list) == 1
