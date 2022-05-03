import datetime
import os
import pathlib

from s3_backup import global_settings
from s3_backup.backup_item import BackupItem
from s3_backup.local_file import LocalFile


def test_local_file(testfile):
    filename, content = testfile
    lf = LocalFile(filename)
    assert lf.key() == filename
    m = lf.metadata()
    assert m['size'] == str(len(content))
    assert m['hash']

    assert lf.key() == filename

    lf2 = LocalFile(filename, "special key")
    assert lf2.key() == "special key"


def test_local_file_upload_if_new():
    lf = LocalFile("whatever")
    assert lf.should_upload(None, None) == BackupItem.ShouldUpload.DoUpload


def test_local_file_upload_if_size_changed(testfile):
    filename, content = testfile
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    os.utime(filename, (yesterday.timestamp(), yesterday.timestamp()))

    f = LocalFile(pathlib.Path(filename))
    m = f.metadata()
    m['size'] = '12345568'
    assert f.should_upload(datetime.datetime.now(), m) == BackupItem.ShouldUpload.DoUpload


def test_local_file_upload_if_mtime(testfile):
    filename, content = testfile

    f = LocalFile(filename)
    m = f.metadata()
    m['hash'] = "wrong hash"
    tomorrow = datetime.datetime.now() + datetime.timedelta(days=1)

    global_settings.trust_mtime = True
    assert f.should_upload(tomorrow, m) == BackupItem.ShouldUpload.DontUpload

    global_settings.trust_mtime = False
    assert f.should_upload(tomorrow, m) == BackupItem.ShouldUpload.DoUpload

    global_settings.trust_mtime = True  # restore for next tests


def test_local_file_touch_if_mtime(testfile):
    filename, content = testfile

    f = LocalFile(filename)
    m = f.metadata()
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    assert f.should_upload(yesterday, m) == BackupItem.ShouldUpload.UpdateModificationTimeOnly


def test_local_file_no_upload_if_up_to_date(testfile):
    filename, content = testfile

    f = LocalFile(filename)
    m = f.metadata()
    tomorrow = datetime.datetime.now() + datetime.timedelta(days=1)
    assert f.should_upload(tomorrow, m) == BackupItem.ShouldUpload.DontUpload
