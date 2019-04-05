import datetime
import logging
import os
import sqlite3
import time
import hashlib
import typing

import freezegun
import pytest
import boto3
import moto
import pytz

import s3_backup


logging.getLogger(None).setLevel(logging.INFO-2)
log_file_handler = logging.StreamHandler()
log_file_handler.setFormatter(logging.Formatter(
    fmt="[%(name)s %(levelname)s] %(message)s"
))
logging.getLogger(None).addHandler(log_file_handler)


def test_filename_xform_identity(filepath):
    assert s3_backup.filename_xform("echo -n \"$FILENAME\"", filepath) == filepath


def test_filename_xform_basic(filepath):
    assert s3_backup.filename_xform("echo -n \"prefix/${FILENAME}.gpg\"", filepath) == f"prefix/{filepath}.gpg"


def test_filename_xform_fail(filepath):
    with pytest.raises(OSError):
        s3_backup.filename_xform("/bin/false", filepath)


def test_data_xform_identity(testfile):
    with open(testfile[0], "rb") as f:
        data_xform = s3_backup.DataXform(None, f)
        assert data_xform.read() == testfile[1]


def test_data_xform_identity_explicit(testfile):
    with open(testfile[0], "rb") as f:
        data_xform = s3_backup.DataXform("cat", f)
        assert data_xform.read() == testfile[1]


def test_data_xform_fail(testfile):
    with open(testfile[0], "rb") as f:
        data_xform = s3_backup.DataXform("/bin/false", f)
        with pytest.raises(OSError):
            data_xform.read()


def test_data_xform_tr(testfile):
    with open(testfile[0], "rb") as f:
        data_xform = s3_backup.DataXform("tr -c '' '_'", f)
        assert data_xform.read() != testfile[1]


def test_data_xform_env(testfile):
    with open(testfile[0], "rb") as f:
        data_xform = s3_backup.DataXform("echo \"$ORIG_FILENAME $XFORM_FILENAME\"", f,
                                         {"ORIG_FILENAME": "orig", "XFORM_FILENAME": "xform"})
        assert data_xform.read() == b"orig xform\n"


def test_local_file(testfile):
    f = s3_backup.LocalFile('/', testfile[0])
    assert f.size() == len(testfile[1])
    assert f.mtime() == pytest.approx(time.time(), abs=2)

    my_hash = hashlib.sha256()
    my_hash.update(testfile[1])
    assert f.digest() == f"{{SHA256}}{my_hash.hexdigest()}"


def create_bucket(content: dict,
                  filename_xform: typing.Callable[[str], str] = None,
                  data_xform: typing.Callable[[typing.BinaryIO], typing.BinaryIO] = None,
) -> typing.Tuple[typing.Any, str]:
    if filename_xform is None:
        filename_xform = lambda f: f
    if data_xform is None:
        data_xform = lambda f: f

    bucket_name = 'test'

    boto_session = boto3.Session(aws_access_key_id="dummy", aws_secret_access_key="dummy", region_name="us-east-1")
    s3_client = boto_session.client('s3')
    s3_client.create_bucket(Bucket=bucket_name)

    for key, data in content.items():
        digest = hashlib.sha256()
        digest.update(data)
        digest = digest.hexdigest()

        s3_key = filename_xform(key)
        s3_data = data_xform(data)

        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=s3_data,
            Metadata={
                'plaintext-size': str(len(data)),
                'plaintext-hash': f"{{SHA256}}{digest}",
            },
        )
    return s3_client, bucket_name


@moto.mock_s3
def test_s3_file_fill_cache():
    content = {
        'foo': b"bar",
        'special key': b"hello world",
        'empty': b"",
    }
    s3_client, bucket_name = create_bucket(content)

    digests = {}
    for key, data in content.items():
        digest = hashlib.sha256()
        digest.update(data)
        digests[key] = f"{{SHA256}}{digest.hexdigest()}"

    sqlite_db = sqlite3.connect(':memory:')
    s3_backup.S3File.fill_cache(bucket_name, sqlite_db, s3_client=s3_client)

    db_content = sqlite_db.execute("SELECT `key`, `plaintext_hash` FROM `s3_cache`;").fetchall()
    assert len(db_content) == len(content)
    for row in db_content:
        assert row[1] == digests[row[0]]


def compare_db(sqlite1: sqlite3.Connection, sqlite2: sqlite3.Connection):
    db1 = sqlite1.execute("SELECT * FROM `s3_cache` ORDER BY `key`;").fetchall()
    db2 = sqlite2.execute("SELECT * FROM `s3_cache` ORDER BY `key`;").fetchall()
    assert len(db1) == len(db2)
    for i, db1_entry in enumerate(db1):
        for j, col in enumerate(db1_entry):
            if col != db2[i][j]:
                raise AssertionError(f"Row for `{db1_entry[0]}` does not match: column {j}: `{col}` != `{db2[i][j]}`")


@moto.mock_s3
def test_sync_scenario(tmp_path):
    os.environ['TZ'] = 'UTC'  # Set system time to UTC. This avoids TimeZone issues with FreezeGun
    time.tzset()
    s3_backup.File.trust_mtime = False  # we can't fake stat()'s return easily

    initial_datetime = datetime.datetime(year=2019, month=1, day=1,
                                         hour=0, minute=0, second=0)
    with freezegun.freeze_time(initial_datetime) as frozen_datetime:
        with open(f"{tmp_path}/a", "wb") as f:
            f.write(b"File a")
        with open(f"{tmp_path}/b", "wb") as f:
            f.write(b"File b")
        os.mkdir(f"{tmp_path}/C")
        with open(f"{tmp_path}/C/d", "wb") as f:
            f.write(b"File d in Dir C")
        timestamp = [
            datetime.datetime.now(pytz.utc)
        ]

        s3_client, bucket_name = create_bucket({})
        cache_db = sqlite3.connect(':memory:')

        def sync():
            s3_backup.do_sync(
                local_path=str(tmp_path),
                s3_bucket=bucket_name,
                storage_class="STANDARD",
                cache_db=cache_db,
                s3_client=s3_client,
                filename_xform_cmd="echo -n \"${FILENAME}.invcase\"",
                data_xform_cmd="tr '[A-Za-z]' '[a-zA-Z]'",  # switch case
            )
        sync()

        bucket_content = s3_client.list_objects_v2(Bucket=bucket_name)
        assert len(bucket_content['Contents']) == 3
        assert s3_client.get_object(Bucket=bucket_name, Key="a.invcase")['Body'].read() == b'fILE A'

        obj = s3_client.get_object(Bucket=bucket_name, Key="b.invcase")
        assert obj['Body'].read() == b'fILE B'
        assert obj['LastModified'] == timestamp[0]

        assert s3_client.get_object(Bucket=bucket_name, Key="C/d.invcase")['Body'].read() == b'fILE D IN dIR c'


        fresh_cache_db = sqlite3.connect(':memory:')
        s3_backup.S3File.fill_cache(bucket_name, fresh_cache_db, s3_client)
        compare_db(cache_db, fresh_cache_db)


        # TIMESTAMP 1:  Now add & delete a file
        frozen_datetime.tick(60)
        timestamp.append(datetime.datetime.now(pytz.utc))
        os.unlink(f"{tmp_path}/a")
        with open(f"{tmp_path}/e", "wb") as f:
            f.write(b"File e")

        sync()

        bucket_content = s3_client.list_objects_v2(Bucket=bucket_name)
        assert len(bucket_content['Contents']) == 3  # check if a is deleted
        assert s3_client.get_object(Bucket=bucket_name, Key="b.invcase")['LastModified'] == timestamp[0]  # Unchanged
        assert s3_client.get_object(Bucket=bucket_name, Key="e.invcase")['Body'].read() == b'fILE E'


        # TIMESTAMP 2:  Update a file
        frozen_datetime.tick(60)
        timestamp.append(datetime.datetime.now(pytz.utc))
        with open(f"{tmp_path}/b", "wb") as f:
            f.write(b"File b, updated")

        sync()

        bucket_content = s3_client.list_objects_v2(Bucket=bucket_name)
        assert len(bucket_content['Contents']) == 3
        assert s3_client.get_object(Bucket=bucket_name, Key="b.invcase")['Body'].read() == b'fILE B, UPDATED'
