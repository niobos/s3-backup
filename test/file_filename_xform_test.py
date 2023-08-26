import pytest

from s3_backup.file import File


def test_filename_xform_identity(filepath):
    File.filename_xform_command = "echo -n \"$FILENAME\""
    f = File('/base', filepath, 'bucket', None)
    assert f.s3_key == filepath

def test_filename_xform_basic(filepath):
    File.filename_xform_command = "echo -n \"prefix/${FILENAME}.gpg\""
    f = File('/base', filepath, 'bucket', None)
    assert f.s3_key == f"prefix/{filepath}.gpg"


def test_filename_xform_fail(filepath):
    File.filename_xform_command = "/bin/false"
    f = File('/base', filepath, 'bucket', None)
    with pytest.raises(OSError):
        _ = f.s3_key
