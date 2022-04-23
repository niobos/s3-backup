import pathlib

import pytest

from s3_backup.data_transform import DataTransform
from s3_backup.local_file import LocalFile


def test_fileobj(testfile):
    filename, content = testfile
    lf = LocalFile(filename)
    xf = DataTransform("cat", lf)
    with xf.fileobj() as f:
        xfcontent = f.read()
    assert xfcontent == content


def test_metadata(testfile):
    filename, content = testfile
    lf = LocalFile(filename)
    xf = DataTransform("cat", lf)
    lfmeta = lf.metadata()
    xfmeta = xf.metadata()
    for k, v in lfmeta.items():
        assert xfmeta[f"plaintext-{k}"] == v


def test_fail(testfile):
    filename, content = testfile
    lf = LocalFile(filename)
    xf = DataTransform("/bin/false", lf)
    with xf.fileobj() as f:
        with pytest.raises(OSError):
            f.read()


def test_env(testfile):
    filename, content = testfile
    lf = LocalFile(filename)
    xf = DataTransform("echo \"$KEY\"", lf)
    with xf.fileobj() as f:
        xfcontent = f.read()
    xfcontent = xfcontent.decode('utf-8').rstrip()
    assert xfcontent == filename

