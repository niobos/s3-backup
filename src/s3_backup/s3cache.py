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
    metadata: typing.Dict[str, str] = {}


class S3cache:
    def __init__(self,
                 cache_db: sqlite3.Connection,
                 ):
        self.cache_db = cache_db

        self._upgrade_schema()  # may raise

    def _upgrade_schema(self):
        cursor = self.cache_db.execute("SELECT `name` FROM `sqlite_master` WHERE `type`='table' AND `name`='s3_object_info';")
        table_exists = cursor.fetchone()
        if table_exists is not None:
            # v2 exists, done
            return

        cursor = self.cache_db.execute("SELECT `name` FROM `sqlite_master` WHERE `type`='table' AND `name`='s3_cache';")
        table_exists = cursor.fetchone()
        if table_exists is not None:
            logger.warning("Upgrading cache schema from v1 -> v2...")
            with self.cache_db as transaction:
                transaction.execute("BEGIN TRANSACTION")  # python only inserts a BEGIN when INSERT'ing
                self._create_s3_object_info_table(transaction)
                self._create_s3_metadata_table(transaction)
                transaction.execute("INSERT INTO `s3_object_info` "
                                    "(`key`, `size`, `mtime`) "
                                    "SELECT `key`, `size`, `mtime` "
                                    "FROM `s3_cache`;")
                transaction.execute("INSERT INTO `s3_metadata` "
                                    "(`key`, `name`, `value`) "
                                    "SELECT `key`, 'plaintext-size', `plaintext_size`"
                                    "FROM `s3_cache`;")
                transaction.execute("INSERT INTO `s3_metadata` "
                                    "(`key`, `name`, `value`) "
                                    "SELECT `key`, 'plaintext-hash', `plaintext_hash`"
                                    "FROM `s3_cache`;")
                transaction.execute("DROP TABLE `s3_cache`;")
            return

        raise ValueError("No cache found")

    @staticmethod
    def _create_s3_object_info_table(transaction):
        transaction.execute(
            "CREATE TABLE `s3_object_info` ("
            "key TEXT PRIMARY KEY NOT NULL, "
            "size INTEGER NOT NULL, "
            "mtime INTEGER NOT NULL"
            ");")

    @staticmethod
    def _create_s3_metadata_table(transaction):
        transaction.execute(
            "CREATE TABLE `s3_metadata` ("
            "key TEXT NOT NULL, "
            "name TEXT NOT NULL, "
            "value TEXT NOT NULL, "
            "PRIMARY KEY(key, name)"
            ");")

    @classmethod
    def initialize_cache(cls,
                         cache_db: sqlite3.Connection,
                         bucket: str,
                         s3_client=None
                         ) -> "S3cache":
        if s3_client is None:
            s3_client = boto3.client('s3')

        logger.info(f"Filling S3 cache for s3://{bucket}...")
        with cache_db as transaction:  # auto-commit at end of block
            transaction.execute("BEGIN TRANSACTION")  # python only inserts a BEGIN when INSERT'ing
            transaction.execute("DROP TABLE IF EXISTS `s3_object_info`;")
            transaction.execute("DROP TABLE IF EXISTS `s3_metadata`;")
            cls._create_s3_object_info_table(transaction)
            cls._create_s3_metadata_table(transaction)

            list_bucket_paginator = s3_client.get_paginator('list_objects_v2')
            for i, page in enumerate(list_bucket_paginator.paginate(Bucket=bucket)):
                logger.log(logging.INFO, f"Parsing bucket list page {i} ({page['KeyCount']} items)...")
                for s3_object in page.get('Contents', []):
                    object_info = s3_client.head_object(
                        Bucket=bucket,
                        Key=s3_object['Key'],
                    )
                    data = {
                        "key": s3_object['Key'],
                        "size": object_info['ContentLength'],
                        "mtime": int(object_info['LastModified'].timestamp()),
                    }
                    logger.log(logging.INFO-2, repr(data))
                    transaction.execute("INSERT INTO `s3_object_info` "
                                        "(`key`, `size`, `mtime`)" +
                                        "VALUES "
                                        "(:key, :size, :mtime)",
                                        data)

                    for name, value in object_info.get('Metadata', {}).items():
                        transaction.execute("INSERT INTO `s3_metadata` "
                                            "(`key`, `name`, `value`)" +
                                            "VALUES "
                                            "(:key, :name, :value)",
                                            {
                                                "key": s3_object['Key'],
                                                "name": name,
                                                "value": value
                                            })

        self = cls(cache_db)
        logger.log(logging.INFO-1, f"Cache filled: {self.summary()}")
        return self

    def summary(self) -> str:
        summary = self.cache_db.execute("SELECT COUNT(`key`), SUM(`size`) FROM `s3_object_info`;").fetchone()
        size = summary[1]
        if size is None:
            size = 0
        return f"{summary[0]} files, {humanize.naturalsize(size, binary=True)}"

    def __getitem__(self, key: str) -> S3ObjectInfo:
        cursor = self.cache_db.execute("SELECT `size`, `mtime` "
                                       "FROM `s3_object_info` "
                                       "WHERE `key` = :key;",
                                       {'key': key})
        row = cursor.fetchone()
        if row is None:
            raise KeyError(f"{key} not found (in cache)")

        cursor = self.cache_db.execute("SELECT `name`, `value` "
                                       "FROM `s3_metadata` "
                                       "WHERE `key` = :key;",
                                       {'key': key})
        metadata = {}
        while True:
            metadatarow = cursor.fetchone()
            if metadatarow is None:
                break
            metadata[metadatarow[0]] = metadatarow[1]

        return S3ObjectInfo(
            s3_size=row[0],
            s3_modification_time=datetime.datetime.fromtimestamp(row[1]),
            metadata=metadata,
        )

    def __setitem__(self, key: str, value: S3ObjectInfo) -> None:
        with self.cache_db as transaction:
            values = {
                "key": key,
                "size": value.s3_size,
                "mtime": int(value.s3_modification_time.timestamp()),
            }
            transaction.execute("INSERT OR REPLACE INTO `s3_object_info` "
                                "(" + ", ".join([f"`{_}`" for _ in values.keys()]) + ")" +
                                "VALUES "
                                "(" + ", ".join([f":{_}" for _ in values.keys()]) + ")",
                                values)

            transaction.execute("DELETE FROM `s3_metadata` WHERE `key` = :key;", {'key': key})
            for name, value in value.metadata.items():
                transaction.execute("INSERT INTO `s3_metadata` "
                                    "(`key`, `name`, `value`)" +
                                    "VALUES "
                                    "(:key, :name, :value)",
                                    {"key": key, "name": name, "value": value})

    def __delitem__(self, key: str) -> None:
        with self.cache_db as transaction:
            transaction.execute("BEGIN TRANSACTION")  # python only inserts a BEGIN when INSERT'ing
            transaction.execute("DELETE FROM `s3_object_info` WHERE `key` = :key;", {'key': key})
            transaction.execute("DELETE FROM `s3_metadata` WHERE `key` = :key;", {'key': key})

    def __contains__(self, item: str) -> bool:
        cursor = self.cache_db.execute("SELECT 1 "
                                       "FROM `s3_object_info` "
                                       "WHERE `key` = :key "
                                       "LIMIT 1;",
                                       {'key': item})
        row = cursor.fetchone()
        return row is not None

    def clear_flags(self) -> None:
        with self.cache_db as transaction:
            transaction.execute("BEGIN TRANSACTION")  # python only inserts a BEGIN when INSERT'ing
            transaction.execute("DROP TABLE IF EXISTS `flag`;")
            transaction.execute("CREATE TEMPORARY TABLE `flag` (key TEXT PRIMARY KEY NOT NULL);")

    def flag(self, key: str) -> None:
        with self.cache_db as transaction:
            transaction.execute("INSERT INTO `flag` (`key`) VALUES (:key);", {'key': key})

    def iterate_unflagged(self) -> typing.Generator[str, None, None]:
        cursor = self.cache_db.execute("SELECT `key` FROM `s3_object_info` "
                                       "WHERE `key` NOT IN ("
                                       "SELECT `key` FROM `flag`"
                                       ")")
        for row in cursor:
            yield row[0]
