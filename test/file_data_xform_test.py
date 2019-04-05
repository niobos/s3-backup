import pytest

from s3_backup.file import DataXform


def test_data_xform_identity(testfile):
    with open(testfile[0], "rb") as f:
        data_xform = DataXform(None, f)
        assert data_xform.read() == testfile[1]


def test_data_xform_identity_explicit(testfile):
    with open(testfile[0], "rb") as f:
        data_xform = DataXform("cat", f)
        assert data_xform.read() == testfile[1]


def test_data_xform_fail(testfile):
    with open(testfile[0], "rb") as f:
        data_xform = DataXform("/bin/false", f)
        with pytest.raises(OSError):
            data_xform.read()


def test_data_xform_tr(testfile):
    with open(testfile[0], "rb") as f:
        data_xform = DataXform("tr -c '' '_'", f)
        assert data_xform.read() != testfile[1]


def test_data_xform_env(testfile):
    with open(testfile[0], "rb") as f:
        data_xform = DataXform("echo \"$ORIG_FILENAME $XFORM_FILENAME\"", f,
                               {"ORIG_FILENAME": "orig", "XFORM_FILENAME": "xform"})
        assert data_xform.read() == b"orig xform\n"

