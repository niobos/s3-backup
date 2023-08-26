import datetime
import logging
import sqlite3
import typing

import attr
import boto3
import humanize


logger = logging.getLogger(__name__)


def int_or_none(something: typing.Any) -> typing.Optional[int]:
    if something is None:
        return None
    return int(something)


@attr.s(auto_attribs=True, slots=True)
class S3ObjectInfo:
    s3_size: int = None
    s3_modification_time: datetime.datetime = None
    plaintext_size: int = None
    plaintext_hash: str = None


class S3cache:
    def __init__(self,
                 cache_db: sqlite3.Connection,
                 ):
        self.cache_db = cache_db

        self._upgrade_schema()  # may raise

    def _upgrade_schema(self):
        cursor = self.cache_db.execute("SELECT `name` FROM `sqlite_master` WHERE `type`='table' AND `name`='s3_cache';")
        table_exists = cursor.fetchone()
        if table_exists is not None:
            # v1 exists, done
            return

        # Check for previous versions

        raise ValueError("No cache found")

    @classmethod
    def initialize_cache(cls,
                         cache_db: sqlite3.Connection,
                         bucket: str,
                         s3_client=None
                         ) -> "S3cache":
        if s3_client is None:
            s3_client = boto3.client('s3')

        logger.info("Filling S3 cache...")
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
                    object_info = s3_client.head_object(
                        Bucket=bucket,
                        Key=s3_object['Key'],
                    )
                    values = {
                        "key": s3_object['Key'],
                        "size": object_info['ContentLength'],
                        "mtime": int(object_info['LastModified'].timestamp()),
                        "plaintext_size": int_or_none(object_info.get('Metadata', {}).get('plaintext-size')),
                        "plaintext_hash": object_info.get('Metadata', {}).get('plaintext-hash'),
                    }
                    logger.log(logging.INFO-2, repr(values))
                    transaction.execute("INSERT OR REPLACE INTO `s3_cache` "
                                        "(" + ", ".join([f"`{_}`" for _ in values.keys()]) + ")" +
                                        "VALUES "
                                        "(" + ", ".join([f":{_}" for _ in values.keys()]) + ")",
                                        values)

        self = cls(cache_db)
        logger.log(logging.INFO-1, f"Cache filled: {self.summary()}")
        return self

    def summary(self) -> str:
        summary = self.cache_db.execute("SELECT COUNT(`key`), SUM(`size`) FROM `s3_cache`;").fetchone()
        size = summary[1]
        if size is None:
            size = 0
        return f"{summary[0]} files, {humanize.naturalsize(size, binary=True)}"

    def __getitem__(self, key: str) -> S3ObjectInfo:
        cursor = self.cache_db.execute("SELECT `size`, `mtime`, `plaintext_size`, `plaintext_hash` "
                                       "FROM `s3_cache` "
                                       "WHERE `key` = :key;",
                                       {'key': key})
        row = cursor.fetchone()
        if row is None:
            raise KeyError(f"{key} not found (in cache)")
        return S3ObjectInfo(
            s3_size=row[0],
            s3_modification_time=datetime.datetime.fromtimestamp(row[1]),
            plaintext_size=row[2],
            plaintext_hash=row[3],
        )

    def __setitem__(self, key: str, value: S3ObjectInfo) -> None:
        with self.cache_db as transaction:
            values = {
                "key": key,
                "size": value.s3_size,
                "mtime": int(value.s3_modification_time.timestamp()),
                "plaintext_size": value.plaintext_size,
                "plaintext_hash": value.plaintext_hash,
            }
            transaction.execute("INSERT OR REPLACE INTO `s3_cache` "
                                "(" + ", ".join([f"`{_}`" for _ in values.keys()]) + ")" +
                                "VALUES "
                                "(" + ", ".join([f":{_}" for _ in values.keys()]) + ")",
                                values)

    def __delitem__(self, key: str) -> None:
        with self.cache_db as transaction:
            transaction.execute("DELETE FROM `s3_cache` WHERE `key` = :key;", {'key': key})

    def __contains__(self, item: str) -> bool:
        try:
            self.__getitem__(item)
            return True
        except KeyError:
            return False

    def clear_flags(self) -> None:
        with self.cache_db as transaction:
            transaction.execute("BEGIN TRANSACTION")  # python only inserts a BEGIN when INSERT'ing
            transaction.execute("DROP TABLE IF EXISTS `flag`;")
            transaction.execute("CREATE TEMPORARY TABLE `flag` (key TEXT PRIMARY KEY NOT NULL);")

    def flag(self, key: str) -> None:
        with self.cache_db as transaction:
            transaction.execute("INSERT INTO `flag` (`key`) VALUES (:key);", {'key': key})

    def iterate_unflagged(self) -> typing.Generator[str, None, None]:
        cursor = self.cache_db.execute("SELECT `key` FROM `s3_cache` "
                                       "WHERE `key` NOT IN ("
                                       "SELECT `key` FROM `flag`"
                                       ")")
        for row in cursor:
            yield row[0]
