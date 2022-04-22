import datetime
import sqlite3

import boto3
import moto
import pytest

from s3_backup.s3cache import S3cache, S3ObjectInfo


@moto.mock_s3
def test_fill_cache():
    bucket_name = 'test'

    boto_session = boto3.Session(aws_access_key_id="dummy", aws_secret_access_key="dummy", region_name="us-east-1")
    s3_client = boto_session.client('s3')
    s3_client.create_bucket(Bucket=bucket_name)

    s3_client.put_object(
        Bucket=bucket_name,
        Key='a',
        Body=b'abcde ',
        Metadata={
            'plaintext-size': '5',
            'plaintext-hash': '{rand}abcde',
            'other': 'foobar',
        }
    )
    s3_client.put_object(
        Bucket=bucket_name,
        Key='"missing metadata"',
        Body=b'12345',
    )

    sqlite_db = sqlite3.connect(':memory:')

    with pytest.raises(ValueError):
        c = S3cache(cache_db=sqlite_db)

    c = S3cache.initialize_cache(
        cache_db=sqlite_db,
        bucket=bucket_name,
        s3_client=s3_client,
    )

    rows = sqlite_db.execute("SELECT `key`, `size` FROM `s3_object_info` ORDER BY `key`;").fetchall()
    assert len(rows) == 2

    assert rows[0][0] == '"missing metadata"'
    assert rows[0][1] == 5
    assert c['"missing metadata"'].s3_size == 5
    # assert rows[0][2] is None
    # assert c['"missing metadata"'].plaintext_size is None

    assert rows[1][0] == 'a'
    assert rows[1][1] == 6
    assert c['a'].s3_size == 6
    # assert rows[1][2] == 5
    # assert c['a'].plaintext_size == 5

    metadata = sqlite_db.execute("SELECT `name`, `value` FROM `s3_metadata` WHERE `key` = \"a\" ORDER BY `name`;").fetchall()
    assert len(metadata) == 3
    assert metadata[0][0] == "other"
    assert metadata[0][1] == "foobar"
    assert metadata[1][0] == "plaintext-hash"
    assert metadata[1][1] == "{rand}abcde"
    assert metadata[2][0] == "plaintext-size"
    assert metadata[2][1] == "5"

    assert 'a' in c


@moto.mock_s3
def test_flagging():
    bucket_name = 'test'

    boto_session = boto3.Session(aws_access_key_id="dummy", aws_secret_access_key="dummy", region_name="us-east-1")
    s3_client = boto_session.client('s3')
    s3_client.create_bucket(Bucket=bucket_name)

    sqlite_db = sqlite3.connect(':memory:')

    c = S3cache.initialize_cache(
        cache_db=sqlite_db,
        bucket=bucket_name,
        s3_client=s3_client,
    )

    c['a'] = S3ObjectInfo(1, datetime.datetime.now())
    c['b'] = S3ObjectInfo(2, datetime.datetime.now())
    c['c'] = S3ObjectInfo(3, datetime.datetime.now())

    c.clear_flags()
    c.flag('a')

    unflagged = set(c.iterate_unflagged())
    assert unflagged == {'b', 'c'}
