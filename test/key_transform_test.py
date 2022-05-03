import pytest

from s3_backup.key_transform import KeyTransformCmd, KeyTransformSubWrapper
from s3_backup.local_file import LocalFile


def test_filename_xform_identity(filepath):
    lf = LocalFile(filepath)
    xf = KeyTransformCmd(
        "echo -n \"$KEY\"",
        lf,
    )
    assert xf.key() == lf.key() == filepath


def test_filename_xform_basic(filepath):
    lf = LocalFile(filepath)
    xf = KeyTransformCmd(
        "echo -n \"prefix/${KEY}.gpg\"",
        lf,
    )
    assert xf.key() == f"prefix/{filepath}.gpg"


def test_filename_xform_fail(filepath):
    lf = LocalFile(filepath)
    xf = KeyTransformCmd(
        "/bin/false",
        lf,
    )
    with pytest.raises(OSError):
        _ = xf.key()


def test_fileobj(testfile):
    filename, content = testfile
    lf = LocalFile(filename)
    xf = KeyTransformCmd("echo -n \"$KEY\"", lf)
    with xf.fileobj() as f:
        xfcontent = f.read()
    assert xfcontent == content


def test_metadata(testfile):
    filename, content = testfile
    lf = LocalFile(filename)
    xf = KeyTransformCmd("echo -n \"$KEY\"", lf)
    assert xf.metadata() == lf.metadata()


class MockItem:
    def __init__(self, key: str):
        self._key = key

    def key(self) -> str:
        return self._key


def test_exclude_re():
    items = [
        MockItem("foo"),
        MockItem("barftest"),
    ]
    remaining_items = list(KeyTransformSubWrapper(items, '^f.*', ''))
    assert len(remaining_items) == 1
    assert remaining_items[0] == items[1]


def test_add_key_suffix():
    items = [
        MockItem("foo"),
        MockItem("bar"),
    ]
    renamed_items = list(KeyTransformSubWrapper(items, '(.*)', '\\1.test'))
    assert renamed_items[0].key() == "foo.test"
    assert renamed_items[1].key() == "bar.test"
    assert len(renamed_items) == 2
