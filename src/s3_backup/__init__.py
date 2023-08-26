import logging
import os.path
import sqlite3
import typing

import boto3
import humanize

from .__meta__ import __version__  # export package-wide
from .file import File, IgnoreThisFile, FileDoesNotExist
from .s3cache import S3cache


logger = logging.getLogger(__name__)


def list_files(base_path: str) -> typing.Generator[str, None, None]:
    """
    Generator yielding files in the given directory (recursively)

    TODO: what does it do with symlinks?
    """
    if base_path.endswith('/'):
        base_path = base_path[:-1]

    for dirname, dirs, files in os.walk(base_path):
        for file in files:
            file_path = os.path.join(dirname, file)
            relative_file_path = file_path[(len(base_path)+1):]
            yield relative_file_path


def do_sync(
        local_path: str,
        s3_bucket: str,
        cache_db: sqlite3.Connection,
        storage_class: str = "STANDARD",
        s3_client=None,
):
    if s3_client is None:
        s3_client = boto3.client('s3')

    try:
        cache = S3cache(cache_db=cache_db)
    except ValueError:
        logger.warning("No cache found, doing full scan of S3...")
        cache = S3cache.initialize_cache(
            cache_db=cache_db,
            bucket=s3_bucket,
            s3_client=s3_client,
        )

    logger.info("Beginning scan of local filesystem")
    cache.clear_flags()
    for relative_filename in list_files(local_path):
        logger.log(logging.INFO-1, f"Processing `{relative_filename}`")

        file = File(
            base_path=local_path,
            relative_path=relative_filename,
            s3_bucket=s3_bucket,
            s3_cache=cache,
            s3_client=s3_client,
        )

        logger.log(logging.INFO-1, f"Transformed filename `{relative_filename}` => `{file.s3_key}`"
                                   f"{ 'IGNORED' if file.ignore else ''}")

        reason = file.upload_needed()
        logger.log(logging.INFO-1, f"Should upload? {reason}")
        if reason:
            logger.info(f"Uploading `{relative_filename}` ("
                        f"{humanize.naturalsize(file.plaintext_size, binary=True)}) "
                        f"to s3://{file.s3_bucket}/{file.s3_key} ({reason})")
            file.do_upload(storage_class=storage_class)

        if file.s3_key != "":
            # s3_key == "" indicates this file should be assumed not to exist locally
            # No need to flag anyway (we'll run in to the UNIQUE constraint anyway)
            cache.flag(file.s3_key)

    logger.info("Deleting S3 objects not corresponding to local files (anymore)...")
    for key in cache.iterate_unflagged():
        logger.log(logging.INFO-1, f"Deleting `{key}`")
        s3_client.delete_object(
            Bucket=s3_bucket,
            Key=key,
        )
    logger.info("Delete done")

    logger.info(f"S3 cache contains: {cache.summary()}")
