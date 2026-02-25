import datetime
import logging
import os.path
import pathlib
import sqlite3
import typing

import boto3
import humanize

from . import global_settings
from .__meta__ import __version__  # export package-wide
from .backup_item import BackupItem
from .local_file import LocalFile
from .s3cache import S3cache, S3ObjectInfo
from .stats import Stats


logger = logging.getLogger(__name__)


class FileScanner:
    def __init__(self, base_path: typing.Union[pathlib.Path, str]):
        self.base_path = str(pathlib.PurePath(base_path))  # will never have a trailing /
        self.files_scanned = 0
        self.bytes_scanned = 0

    def summary(self) -> str:
        return f"Scanned {self.files_scanned} files, totalling {humanize.naturalsize(self.bytes_scanned, binary=True)}"

    @staticmethod
    def _recursive_scandir(dir: str) -> typing.Generator[os.DirEntry, None, None]:
        with os.scandir(dir) as it:
            for entry in it:
                if entry.is_dir():
                    yield from FileScanner._recursive_scandir(os.path.join(dir, entry.name))
                else:
                    yield entry

    def __iter__(self) -> typing.Generator[LocalFile, None, None]:
        base_path_len = len(self.base_path)
        for entry in self._recursive_scandir(self.base_path):
            assert entry.path.startswith(self.base_path), f"Path outside basedir: {entry.path}"
            try:
                f = LocalFile(path=entry.path, key=entry.path[(base_path_len+1):])  # +1 for '/'
                self.bytes_scanned += f.stat().st_size  # may raise
                self.files_scanned += 1  # do stat() first, so this count is correct when it raises
                yield f
            except FileNotFoundError:
                logger.warning(f"File vanished before we could backup: {entry.path}")


def do_sync(
        file_list: typing.Iterator[BackupItem],
        s3_bucket: str,
        cache_db: sqlite3.Connection,
        storage_class: str = "STANDARD",
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

        try:
            upload_needed = item.should_upload(
                s3_info.s3_modification_time if s3_info is not None else None,
                s3_info.metadata if s3_info is not None else None,
            )
            logger.log(logging.INFO-1, f"Should upload? {upload_needed.name}")
            if upload_needed == BackupItem.ShouldUpload.DoUpload:
                size = do_upload(
                    item,
                    s3_bucket,
                    s3_client,
                    cache,
                    storage_class,
                )
                stats.upload(size)

            cache.flag(item.key())
        except FileNotFoundError:
            logger.warning(f"File vanished before we could backup: {item}")
        except PermissionError as e:
            logger.warning(f"{e}; skipping: {item}")

    logger.info("Deleting S3 objects not corresponding to local files (anymore)...")
    for key in cache.iterate_unflagged():
        logger.info(f"{'DRY RUN ' if global_settings.dry_run else ''}Deleting `{key}`")
        if not global_settings.dry_run:
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
) -> int:
    logger.info(f"{'DRY RUN ' if global_settings.dry_run else ''}"
                f"Uploading {item} "
                f"to s3://{s3_bucket}/{item.key()}")
    with item.fileobj() as f:
        counted_f = ByteCounter(f)
        metadata = item.metadata()

        if isinstance(metadata.get('size'), BackupItem.SizeMetadata):
            del metadata['size']

        if global_settings.dry_run:
            while True:
                _ = counted_f.read(1024)
                if len(_) == 0:
                    break
        else:
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

    logger.info(f"{'DRY RUN ' if global_settings.dry_run else ''}"
                f"Uploaded s3://{s3_bucket}/{item.key()} ({humanize.naturalsize(counted_f.bytes, binary=True)})")

    return counted_f.bytes
