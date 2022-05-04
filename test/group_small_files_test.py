import contextlib
import datetime
import io
import typing
import zipfile

from s3_backup import BackupItem
from s3_backup.group_small_files import group_files, GroupedItem


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

    def hash(self) -> str:
        return self._key


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


class MemoryItem(BackupItem):
    def __init__(self, key: str, content: bytes):
        self._key = key
        self.content = content

    def key(self) -> str:
        return self._key

    def mtime(self) -> typing.Optional[float]:
        return datetime.datetime.now(tz=datetime.timezone.utc).timestamp()

    @contextlib.contextmanager
    def fileobj(self) -> typing.Generator[typing.BinaryIO, None, None]:
        b = io.BytesIO(self.content)
        yield b

    def should_upload(
            self,
            modification_time: typing.Optional[datetime.datetime],
            metadata: typing.Optional[typing.Mapping[str, str]],
    ) -> BackupItem.ShouldUpload:
        return BackupItem.ShouldUpload.DoUpload

    def hash(self) -> str:
        return self._key


def test_zip():
    g = GroupedItem("test", [MemoryItem("foo", b"bar")])
    with g.fileobj() as f:
        b = f.read()

    b_fl = io.BytesIO(b)
    with zipfile.ZipFile(b_fl, 'r') as f:
        assert len(f.infolist()) == 1
        assert f.infolist()[0].orig_filename == "foo"
        o = f.read("foo")
        assert o == b"bar"
