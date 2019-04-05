import datetime
import logging
import os.path
import sqlite3
import typing
import hashlib
import subprocess

import boto3
import humanize

from .__meta__ import __version__  # export package-wide


logger = logging.getLogger(__name__)


class File:
    trust_mtime = True

    def __init__(self):
        self._digest = {}

    def exists(self) -> bool:
        raise NotImplementedError()

    def mtime(self) -> datetime.datetime:
        raise NotImplementedError()

    def size(self) -> int:
        raise NotImplementedError()

    @staticmethod
    def _resolve_digest_algorithm(algorithm: str = None):
        if algorithm == "SHA256":
            return "SHA256", hashlib.sha256()
        else:
            return "SHA256", hashlib.sha256()

    def digest_(self, algorithm: str, digest) -> str:
        raise NotImplementedError()

    def digest(self, algorithm: str = None) -> str:
        algorithm, digest = self._resolve_digest_algorithm(algorithm)
        if algorithm not in self._digest:
            self._digest[algorithm] = self.digest_(algorithm, digest)
        return self._digest[algorithm]

    def supersedes(self, other: typing.Any) -> typing.Union[str, bool]:
        """
        Compare the two File's to find out if `self` should supersede `other`
        """
        if not isinstance(other, File):
            raise TypeError(f"Can't compare {type(self)} to {type(other)}")

        if self.exists() and not other.exists():
            return "does not exist on other side"

        if self.trust_mtime and self.mtime() > other.mtime():
            return f"is newer ({self.mtime()} > {other.mtime()})"  # self is newer, and mtimes are trustworthy

        if self.size() != other.size():
            return f"different size ({self.size()} != {other.size()})"

        # Check which (if any) digests are already calculated for `self` and `other`
        # Prefer digests calculated by both, other and self (in that order)
        self_digests = set(self._digest.keys())
        other_digests = set(other._digest.keys())
        common_digests = self_digests.intersection(other_digests)

        digest_to_compare = None
        if len(common_digests) > 0:
            digest_to_compare = common_digests.pop()
        elif len(other_digests) > 0:
            digest_to_compare = other_digests.pop()
        elif len(self_digests) > 0:
            digest_to_compare = self_digests.pop()

        self_digest = self.digest(digest_to_compare)
        other_digest = other.digest(digest_to_compare)
        if self_digest != other_digest:
            logger.debug(f"Uploading `{str(self)}` because its digest differ")
            return f"different digests ({self_digest} != {other_digest})"

        return False


class LocalFile(File):
    def __init__(self, path: str, filename: str):
        super().__init__()
        self.path = path
        self.filename = filename
        self._stat = None

    def __str__(self) -> str:
        return self.filename

    @property
    def full_filename(self):
        return os.path.join(self.path, self.filename)

    def stat(self) -> os.stat_result:
        if self._stat is None:
            self._stat = os.stat(self.full_filename)
        return self._stat

    def mtime(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(self.stat().st_mtime)

    def size(self) -> int:
        return self.stat().st_size

    def exists(self) -> bool:
        try:
            self.stat()
            return True
        except FileNotFoundError:
            return False

    def digest_(self, algorithm: str, digest) -> str:
        with open(self.full_filename, "rb") as f:
            while True:
                data = f.read(1024*1024)
                if not data:
                    break
                digest.update(data)
        return f"{{{algorithm}}}{digest.hexdigest()}"


def int_or_none(something: typing.Any) -> typing.Optional[int]:
    if something is None:
        return None
    return int(something)


class S3File(File):
    def __init__(self,
                 bucket: str,
                 key: str,
                 cache_db: sqlite3.Connection,
    ):
        super().__init__()
        self.bucket = bucket
        self.key = key
        self.cache_db = cache_db

    def __str__(self) -> str:
        return f"s3://{self.bucket}/{self.key}"

    @staticmethod
    def upgrade_schema(cache_db: sqlite3.Connection):
        # check for V1
        cursor = cache_db.execute("SELECT `name` FROM `sqlite_master` WHERE `type`='table' AND `name`='s3_cache';")
        table_exists = cursor.fetchone()
        if table_exists is not None:
            # v1 exists, done
            return

        # Check for previous versions

        raise ValueError("No cache found")

    @classmethod
    def fill_cache(cls,
                   bucket: str,
                   cache_db: sqlite3.Connection,
                   s3_client=None,
    ):
        if s3_client is None:
            s3_client = boto3.client('s3')

        with cache_db as transaction:  # auto-commit at end of block
            transaction.execute("BEGIN TRANSACTION")  # python only inserts a BEGIN when INSERT'ing
            transaction.execute("DROP TABLE IF EXISTS `s3_cache`;")
            transaction.execute(
                "CREATE TABLE `s3_cache` ("
                "key TEXT PRIMARY KEY NOT NULL, "
                "size INTEGER NOT NULL, "
                "mtime INTEGER NOT NULL, "
                "plaintext_size INTEGER, "
                "plaintext_hash TEXT"
                ");")

            list_bucket_paginator = s3_client.get_paginator('list_objects_v2')
            for i, page in enumerate(list_bucket_paginator.paginate(Bucket=bucket)):
                logger.log(logging.INFO-1, f"Parsing bucket list page {i} ({page['KeyCount']} items)...")
                for s3_object in page.get('Contents', []):
                    cls(bucket=bucket, key=s3_object['Key'], cache_db=transaction)._update_cache(s3_client=s3_client)

        num = cache_db.execute("SELECT COUNT(`key`) FROM `s3_cache`;").fetchone()[0]
        logger.log(logging.INFO-1, f"Saved {num} objects to cache")

    def _update_cache(self, s3_client):
        object_info = s3_client.head_object(
            Bucket=self.bucket,
            Key=self.key,
        )
        values = {
            "key": self.key,
            "size": object_info['ContentLength'],
            "mtime": int(object_info['LastModified'].timestamp()),
            "plaintext_size": int_or_none(object_info.get('Metadata', {}).get('plaintext-size')),
            "plaintext_hash": object_info.get('Metadata', {}).get('plaintext-hash'),
        }
        logger.log(logging.INFO-2, repr(values))
        self.cache_db.execute("INSERT OR REPLACE INTO `s3_cache` "
                              "(" + ", ".join([f"`{_}`" for _ in values.keys()]) + ")" +
                              "VALUES "
                              "(" + ", ".join([f":{_}" for _ in values.keys()]) + ")",
                              values)

    @staticmethod
    def summary(cache_db: sqlite3.Connection):
        summary = cache_db.execute("SELECT COUNT(`key`), SUM(`size`) FROM `s3_cache`;").fetchone()
        return {
            'numfiles': summary[0],
            'total size': summary[1],
        }

    @staticmethod
    def start_seen(cache_db: sqlite3.Connection):
        with cache_db as transaction:
            transaction.execute("BEGIN TRANSACTION")  # python only inserts a BEGIN when INSERT'ing
            transaction.execute("DROP TABLE IF EXISTS `seen`;")
            transaction.execute("CREATE TEMPORARY TABLE `seen` (key TEXT PRIMARY KEY NOT NULL);")

    @staticmethod
    def mark_as_seen(cache_db: sqlite3.Connection, key: str):
        with cache_db as transaction:
            transaction.execute("INSERT INTO `seen` (`key`) VALUES (:key);", {'key': key})

    @staticmethod
    def delete_unseen(cache_db: sqlite3.Connection, s3_client, bucket: str):
        cursor = cache_db.execute("SELECT `key` FROM `s3_cache` "
                                  "WHERE `key` NOT IN ("
                                  "SELECT `key` FROM `seen`"
                                  ")")
        for row in cursor:
            key = row[0]
            logger.log(logging.INFO-1, f"Deleting `{key}`")
            s3_client.delete_object(Bucket=bucket, Key=key)
            with cache_db as transaction:
                transaction.execute("DELETE FROM `s3_cache` WHERE `key` = :key;", {'key': key})

    def replace(self,
                file: LocalFile,
                storage_class: str,
                s3_client,
                data_xform_cmd: typing.Optional[str] = None,
    ):
        with open(file.full_filename, "rb") as f:
            f = DataXform(
                data_xform_cmd, f,
                extra_env={
                    'ORIG_FILENAME': file.filename,
                    'XFORM_FILENAME': self.key,
                },
            )

            _ = s3_client.upload_fileobj(
                Fileobj=f,
                Bucket=self.bucket,
                Key=self.key,
                ExtraArgs={
                    'StorageClass': storage_class,
                    'Metadata': {
                        'plaintext-size': str(file.size()),
                        'plaintext-hash': file.digest(),
                    }
                },
            )

        values = {
            "key": self.key,
            "size": f.size,
            "mtime": int(datetime.datetime.now().timestamp()),
            "plaintext_size": file.size(),
            "plaintext_hash": file.digest(),
        }
        with self.cache_db as transaction:
            transaction.execute("INSERT OR REPLACE INTO `s3_cache` "
                                "(" + ", ".join([f"`{_}`" for _ in values.keys()]) + ")" +
                                "VALUES "
                                "(" + ", ".join([f":{_}" for _ in values.keys()]) + ")",
                                values)

    def exists(self) -> bool:
        cursor = self.cache_db.execute("SELECT `key` FROM `s3_cache` WHERE `key` = :key", {'key': self.key})
        row = cursor.fetchone()
        return row is not None

    def mtime(self) -> datetime.datetime:
        cursor = self.cache_db.execute("SELECT `mtime` FROM `s3_cache` WHERE `key` = :key", {'key': self.key})
        row = cursor.fetchone()
        if row is None:
            raise KeyError()
        return datetime.datetime.fromtimestamp(row[0])

    def size(self) -> int:
        cursor = self.cache_db.execute("SELECT `plaintext_size` FROM `s3_cache` WHERE `key` = :key", {'key': self.key})
        row = cursor.fetchone()
        if row is None:
            raise KeyError()
        return row[0]

    def digest_(self, algorithm: str, digest) -> str:
        cursor = self.cache_db.execute("SELECT `plaintext_hash` FROM `s3_cache` WHERE `key` = :key", {'key': self.key})
        row = cursor.fetchone()
        if row is None:
            raise KeyError()
        if row.startswith(f"{{{algorithm}}}"):
            return row[0]
        else:
            raise ValueError()


def list_files(base_path: str) -> typing.Generator[LocalFile, None, None]:
    """
    Generator yielding files in the given directory (recursively)

    TODO: what does it do with symlinks?
    """
    if base_path.endswith('/'):
        base_path = base_path[:-1]

    for dirname, dirs, files in os.walk(base_path, followlinks=True):
        for file in files:
            file_path = os.path.join(dirname, file)
            relative_file_path = file_path[(len(base_path)+1):]
            yield LocalFile(base_path, relative_file_path)


def filename_xform(xform: typing.Optional[str], filename: str) -> str:
    """
    Transforms filename with the given transform
    :raises: OSError when the transform call fails
    """
    if xform is None:
        return filename

    xform_env = os.environ.copy()
    xform_env['FILENAME'] = filename
    xform = subprocess.run(
        ["/bin/bash", "-c", xform],
        input=filename,
        encoding='utf-8',
        capture_output=True,
        env=xform_env,
    )

    if xform.returncode != 0:
        raise OSError(xform.stderr)

    return xform.stdout


class DataXform:
    """
    Helper class to write Upload an S3 object while its content is generated on
    the fly.

    s3_client.upload_fileobj() will read() from the given object when it needs
    more data. This class will read from the transform-subprocess.
    """
    def __init__(self,
                 xform: typing.Optional[str],
                 fileobj: typing.BinaryIO,
                 extra_env: dict = None
    ):
        xform_env = os.environ.copy()
        if extra_env is not None:
            xform_env.update(extra_env)

        if xform is not None:
            self.subprocess = subprocess.Popen(
                ["/bin/bash", "-c", xform],
                stdin=fileobj,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=xform_env,
            )
            self.output = self.subprocess.stdout
        else:
            self.subprocess = None
            self.output = fileobj

        self.size = 0

    def read(self, size=-1) -> bytes:
        data = self.output.read(size)

        self.size += len(data)

        if len(data) == 0 and self.subprocess is not None:
            return_code = self.subprocess.wait()
            if return_code != 0:
                raise OSError(self.subprocess.stderr)

        return data


def do_sync(
        local_path: str,
        s3_bucket: str,
        storage_class: str,
        cache_db: sqlite3.Connection,
        filename_xform_cmd: typing.Optional[str] = None,
        data_xform_cmd: typing.Optional[str] = None,
        s3_client=None,
):
    if s3_client is None:
        s3_client = boto3.client('s3')

    try:
        S3File.upgrade_schema(cache_db)
    except ValueError:
        logger.warning("No cache found, doing full scan of S3...")
        S3File.fill_cache(s3_bucket, cache_db, s3_client)

    logger.info("Beginning scan of local filesystem")
    S3File.start_seen(cache_db)
    for local_file in list_files(local_path):
        logger.log(logging.INFO-1, f"Processing `{local_file}`")

        s3_filename = local_file.filename
        if filename_xform_cmd is not None:
            s3_filename = filename_xform(filename_xform_cmd, s3_filename)

        logger.log(logging.INFO-1, f"Transformed filename `{local_file}` => `{s3_filename}`")

        s3_file = S3File(s3_bucket, s3_filename, cache_db)

        reason = local_file.supersedes(s3_file)
        logger.log(logging.INFO-1, f"Should upload? {reason}")
        if reason:
            logger.info(f"Uploading `{local_file}` to s3://{s3_file.bucket}/{s3_file.key} ({reason})")
            s3_file.replace(
                file=local_file,
                storage_class=storage_class,
                s3_client=s3_client,
                data_xform_cmd=data_xform_cmd,
            )

        S3File.mark_as_seen(cache_db, s3_filename)

    logger.info("Deleting S3 objects not corresponding to local files (anymore)...")
    S3File.delete_unseen(cache_db, s3_client, s3_bucket)
    logger.info("Delete done")

    summary = S3File.summary(cache_db)
    logger.info(f"Stored {summary['numfiles']} files, "
                f"totalling {humanize.naturalsize(summary['total size'], binary=True)}")
