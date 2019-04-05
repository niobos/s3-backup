import datetime
import hashlib
import os
import sqlite3

import boto3
import botocore.errorfactory
import moto
import pytest

from s3_backup import do_sync, File, S3cache


@moto.mock_s3
def test_sync(tmp_path):
    File.filename_xform_command = 'echo -n "${FILENAME}.invcase"'
    File.data_xform_command = "tr '[A-Za-z]' '[a-zA-Z]'"

    bucket_name = 'test'

    boto_session = boto3.Session(aws_access_key_id="dummy", aws_secret_access_key="dummy", region_name="us-east-1")
    s3_client = boto_session.client('s3')
    s3_client.create_bucket(Bucket=bucket_name)
    s3_client.put_object(
        Bucket=bucket_name,
        Key="leftover",
        Body=b"leftover file, should be deleted",
    )
    with open(f"{tmp_path}/old", "wb") as f:
        f.write(b"old file, untouched")
    now = datetime.datetime.now().timestamp()
    os.utime(f"{tmp_path}/old", (now-5, now-5))  # needed because of rounding errors
    s3_client.put_object(
        Bucket=bucket_name,
        Key="old.invcase",
        Body=b"old file, untouched",
        Metadata={
            'plaintext-size': str(19),
            'plaintext-hash': f"{{SHA256}}{hashlib.sha256(b'old file, untouched').hexdigest()}"
        },
    )
    s3_client.put_object(
        Bucket=bucket_name,
        Key="modified.invcase",
        Body=b"file modified since last sync",
    )

    with open(f"{tmp_path}/.new", "wb") as f:
        f.write(b"foobar")
    with open(f"{tmp_path}/modified", "wb") as f:
        f.write(b"Hello World")

    s3_cache = sqlite3.Connection(':memory:')

    do_sync(
        local_path=str(tmp_path),
        s3_bucket=bucket_name,
        cache_db=s3_cache,
        s3_client=s3_client,
    )

    with pytest.raises(botocore.errorfactory.ClientError):
        # assert leftover file is deleted
        leftover_file = s3_client.get_object(
            Bucket=bucket_name,
            Key="leftover",
        )

    # assert old file still present
    obj_info = s3_client.get_object(
        Bucket=bucket_name,
        Key='old.invcase',
    )
    assert obj_info['Body'].read() == b"old file, untouched"

    # assert new file uploaded
    obj_info = s3_client.get_object(
        Bucket=bucket_name,
        Key='.new.invcase',
    )
    assert obj_info['Body'].read() == b"FOOBAR"
    assert int(obj_info['Metadata']['plaintext-size']) == 6
    assert obj_info['Metadata']['plaintext-hash'] == f"{{SHA256}}{hashlib.sha256(b'foobar').hexdigest()}"

    # assert modified file uploaded
    obj_info = s3_client.get_object(
        Bucket=bucket_name,
        Key='modified.invcase',
    )
    assert obj_info['Body'].read() == b"hELLO wORLD"
    assert int(obj_info['Metadata']['plaintext-size']) == 11
    assert obj_info['Metadata']['plaintext-hash'] == f"{{SHA256}}{hashlib.sha256(b'Hello World').hexdigest()}"

