import pathlib

import pytest

from s3_backup.key_transform import KeyTransform
from s3_backup.local_file import LocalFile


def test_filename_xform_identity(filepath):
    lf = LocalFile(filepath)
    xf = KeyTransform(
        "echo -n \"$KEY\"",
        lf,
    )
    assert xf.key() == lf.key() == filepath


def test_filename_xform_basic(filepath):
    lf = LocalFile(filepath)
    xf = KeyTransform(
        "echo -n \"prefix/${KEY}.gpg\"",
        lf,
    )
    assert xf.key() == f"prefix/{filepath}.gpg"


def test_filename_xform_fail(filepath):
    lf = LocalFile(filepath)
    xf = KeyTransform(
        "/bin/false",
        lf,
    )
    with pytest.raises(OSError):
        _ = xf.key()


def test_fileobj(testfile):
    filename, content = testfile
    lf = LocalFile(filename)
    xf = KeyTransform("echo -n \"$KEY\"", lf)
    with xf.fileobj() as f:
        xfcontent = f.read()
    assert xfcontent == content


def test_metadata(testfile):
    filename, content = testfile
    lf = LocalFile(filename)
    xf = KeyTransform("echo -n \"$KEY\"", lf)
    assert xf.metadata() == lf.metadata()
