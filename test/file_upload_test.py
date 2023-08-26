import datetime
import sqlite3
import hashlib

import attr
import boto3
import freezegun
import moto

from s3_backup.file import File
from s3_backup.s3cache import S3ObjectInfo, S3cache


@attr.s(auto_attribs=True, slots=True)
class FakeStat:
    st_mtime: float = 0
    st_size: int = 0


def test_upload_needed():
    File.filename_xform_command = None
    File.data_xform_command = None

    initial_datetime = datetime.datetime(year=2019, month=1, day=1,
                                         hour=0, minute=0, second=0)
    with freezegun.freeze_time(initial_datetime) as frozen_datetime:
        s3_cache = {}
        f = File('/base', 'relative', 'bucket',
                 s3_cache,
                 )

        f._cache['stat'] = FakeStat(
            st_mtime=datetime.datetime.now().timestamp(),
            st_size=11,
        )
        f._cache['digest'] = {'rand': "{rand}xxx"}
        assert f.upload_needed().startswith("does not exist on S3")

        s3_cache['relative'] = S3ObjectInfo(
            s3_size=6,
            s3_modification_time=datetime.datetime.now(),
            plaintext_size=11,
            plaintext_hash="{rand}xxx",
        )
        assert not f.upload_needed()

        frozen_datetime.tick(1)
        f._cache['stat'] = FakeStat(
            st_mtime=datetime.datetime.now().timestamp(),
            st_size=11,
        )
        assert not f.upload_needed()  # it is newer, but same content; no upload needed

        frozen_datetime.tick(1)
        s3_cache['relative'].s3_modification_time = datetime.datetime.now()
        assert not f.upload_needed()

        f._cache['stat'] = FakeStat(
            st_mtime=datetime.datetime.now().timestamp(),
            st_size=12,  # Change size
        )
        assert f.upload_needed().startswith("different size")

        frozen_datetime.tick(1)
        s3_cache['relative'].s3_modification_time = datetime.datetime.now()
        s3_cache['relative'].plaintext_size = 12
        assert not f.upload_needed()

        frozen_datetime.tick(1)
        f._cache['stat'] = FakeStat(
            st_mtime=datetime.datetime.now().timestamp(),  # Update mtime to be newer
            st_size=12,
        )
        f._cache['digest'] = {'rand': "{rand}yyy"}  # different hash
        assert f.upload_needed().startswith("different hash")

        f._cache['digest'] = {'sum': '{sum}xxx'}  # incompatible hash
        assert f.upload_needed().startswith("could not get plaintext_hash of S3 object")


@moto.mock_s3
def test_upload(testfile):
    File.filename_xform_command = None
    File.data_xform_command = None

    bucket_name = 'test'

    boto_session = boto3.Session(aws_access_key_id="dummy", aws_secret_access_key="dummy", region_name="us-east-1")
    s3_client = boto_session.client('s3')
    s3_client.create_bucket(Bucket=bucket_name)

    s3_cache = S3cache.initialize_cache(
        cache_db=sqlite3.Connection(':memory:'),
        bucket=bucket_name,
        s3_client=s3_client,
    )

    testfile_name = testfile[0][1:]  # strip leading / of absolute path
    plaintext_hash = f"{{SHA256}}{hashlib.sha256(testfile[1]).hexdigest()}"

    file = File('/', testfile_name, bucket_name, s3_cache, s3_client)
    file.do_upload()

    s3_file = s3_client.get_object(
        Bucket=bucket_name,
        Key=testfile_name,
    )
    assert s3_file['Body'].read() == testfile[1]
    assert int(s3_file['Metadata']['plaintext-size']) == len(testfile[1])
    assert s3_file['Metadata']['plaintext-hash'] == plaintext_hash

    assert testfile_name in s3_cache
    assert s3_cache[testfile_name].plaintext_size == len(testfile[1])
    assert s3_cache[testfile_name].plaintext_hash == plaintext_hash
    assert s3_cache[testfile_name].s3_size == len(testfile[1])
