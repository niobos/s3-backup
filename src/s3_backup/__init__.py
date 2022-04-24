import datetime
import logging
import os.path
import pathlib
import sqlite3
import typing

import boto3
import humanize

from .__meta__ import __version__  # export package-wide
from .backup_item import BackupItem
from .data_transform import DataTransform
from .key_transform import KeyTransform
from .local_file import LocalFile
from .s3cache import S3cache, S3ObjectInfo
from .stats import Stats


logger = logging.getLogger(__name__)


class FileScanner:
    def __init__(self, base_path: typing.Union[pathlib.Path, str]):
        self.base_path = str(pathlib.PurePath(base_path))
        self.files_scanned = 0
        self.bytes_scanned = 0

    def summary(self) -> str:
        return f"Scanned {self.files_scanned} files, totalling {humanize.naturalsize(self.bytes_scanned, binary=True)}"

    @staticmethod
    def _recursive_scandir(dir: str) -> typing.Generator[os.DirEntry, None, None]:
        with os.scandir(dir) as it:
            for entry in it:
                if entry.is_dir():
                    for subentry in FileScanner._recursive_scandir(os.path.join(dir, entry.name)):
                        yield subentry
                else:
                    yield entry

    def __iter__(self) -> typing.Generator[LocalFile, None, None]:
        base_path_len = len(self.base_path)
        for entry in self._recursive_scandir(self.base_path):
            if not entry.path.startswith(self.base_path):
                raise RuntimeError("Path outside basedir: ", entry.path)
            f = LocalFile(path=entry.path, key=entry.path[(base_path_len+1):])  # +1 for '/'
            self.files_scanned += 1
            self.bytes_scanned += f.stat().st_size
            yield f


def do_sync(
        file_list: typing.Iterator[BackupItem],
        s3_bucket: str,
        cache_db: sqlite3.Connection,
        storage_class: str = "STANDARD",
        dry_run: bool = False,
        s3_client=None,
):
    if s3_client is None:
        s3_client = boto3.client('s3')

    try:
        cache = S3cache(cache_db=cache_db)
        logger.info("Cache opened: " + cache.summary())
    except ValueError:
        logger.warning("No cache found, doing full scan of S3...")
        cache = S3cache.initialize_cache(
            cache_db=cache_db,
            bucket=s3_bucket,
            s3_client=s3_client,
        )

    logger.info("Beginning scan of local filesystem")
    cache.clear_flags()
    stats = Stats()
    for item in file_list:
        logger.log(logging.INFO-1, f"Processing {item}")

        try:
            s3_info = cache[item.key()]
            if 'size' not in s3_info.metadata:
                s3_info.metadata['size'] = s3_info.s3_size
        except KeyError:
            s3_info = None

        upload_needed = item.should_upload(
            s3_info.s3_modification_time if s3_info is not None else None,
            s3_info.metadata if s3_info is not None else None,
        )
        logger.log(logging.INFO-1, f"Should upload? {upload_needed.name}")
        if upload_needed == BackupItem.ShouldUpload.DoUpload:
            logger.info(f"Uploading {item} "
                        f"to s3://{s3_bucket}/{item.key()} ({upload_needed.name})"
                        f"{' DRY RUN' if dry_run else ''}")
            size = do_upload(
                item,
                s3_bucket,
                s3_client,
                cache,
                storage_class,
                dry_run,
            )
            stats.upload(size)

        cache.flag(item.key())

    logger.info("Deleting S3 objects not corresponding to local files (anymore)...")
    for key in cache.iterate_unflagged():
        logger.info(f"Deleting `{key}`{' DRY RUN' if dry_run else ''}")
        if not dry_run:
            s3_client.delete_object(
                Bucket=s3_bucket,
                Key=key,
            )
            del cache[key]
        stats.delete()
    logger.info("Delete done")

    logger.log(logging.INFO+1, stats.summary())
    logger.log(logging.INFO+1, f"S3 cache contains: {cache.summary()}")


class ByteCounter:
    def __init__(self, underlying: typing.BinaryIO):
        self.underlying = underlying
        self.bytes = 0

    def read(self, n: int = 0) -> bytes:
        out = self.underlying.read(n)
        self.bytes += len(out)
        return out


def do_upload(
        item: BackupItem,
        s3_bucket: str,
        s3_client,
        s3_cache: S3cache,
        storage_class: str = "STANDARD",
        dry_run: bool = False,
) -> int:
    with item.fileobj() as f:
        counted_f = ByteCounter(f)
        metadata = item.metadata()

        if isinstance(metadata.get('size'), BackupItem.SizeMetadata):
            del metadata['size']

        if dry_run:
            while True:
                _ = counted_f.read(1024)
                if len(_) == 0:
                    break
            return counted_f.bytes
        # else:

        s3_client.upload_fileobj(
            Fileobj=counted_f,
            Bucket=s3_bucket,
            Key=item.key(),
            ExtraArgs={
                'StorageClass': storage_class,
                'Metadata': metadata,
            },
        )

    s3_cache[item.key()] = S3ObjectInfo(
        s3_size=counted_f.bytes,
        s3_modification_time=datetime.datetime.now(),
        metadata=metadata,
    )

    return counted_f.bytes
