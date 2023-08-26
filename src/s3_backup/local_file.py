import contextlib
import datetime
import functools
import hashlib
import logging
import os
import pathlib
import re
import typing

from s3_backup.backup_item import BackupItem


logger = logging.getLogger(__name__)


class LocalFile(BackupItem):
    """
    Represents a local file that needs to be backed up
    """
    trust_mtime = True

    def __init__(
            self,
            path: typing.Union[pathlib.Path, str],
            key: typing.Optional[str] = None,
    ):
        self.path = path

        if key is not None:
            self._key = key
        else:
            self._key = path

    def __str__(self) -> str:
        return repr(self)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} " \
               f"key={self._key} " \
               f"path={self.path}>"

    def key(self) -> str:
        return self._key

    @contextlib.contextmanager
    def fileobj(self) -> typing.Generator[typing.BinaryIO, None, None]:
        with open(self.path, "rb") as f:
            yield f

    def metadata(self) -> typing.Mapping[str, str]:
        return {
            'size': BackupItem.SizeMetadata(self.stat().st_size),
            'hash': self.digest('SHA256'),
        }

    def should_upload(
            self,
            modification_time: typing.Optional[datetime.datetime],
            metadata: typing.Optional[typing.Mapping[str, str]]
    ) -> BackupItem.ShouldUpload:
        if modification_time is None:
            logger.info(f"{self} needs uploading: "
                        f"Not on S3")
            return BackupItem.ShouldUpload.DoUpload

        s3_size = int(metadata.get('size', -1))
        if self.stat().st_size != s3_size:
            logger.info(f"{self} needs uploading: "
                        f"different size ({self.stat().st_size} != {s3_size})")
            return BackupItem.ShouldUpload.DoUpload

        if self.trust_mtime:
            local_mtime = datetime.datetime.fromtimestamp(self.stat().st_mtime)
            if local_mtime < modification_time:
                return BackupItem.ShouldUpload.DontUpload  # mtimes are trustworthy, no need to check hash
            # else: check digest below

        try:
            algorithm = re.match(r'^{([^}]+)}', metadata['hash']).group(1)
            my_digest = self.digest(algorithm)
            if my_digest != metadata['hash']:
                logger.info(f"{self} needs uploading: "
                            f"more recent locally & different hash "
                            f"({my_digest} != {metadata['hash']})")
                return BackupItem.ShouldUpload.DoUpload
        except (AttributeError,  # regex doesn't match
                TypeError,  # plaintext_hash is None
                ValueError,  # algorithm isn't known
                ) as e:
            logger.warning(f"{self} needs uploading: could not get plaintext_hash of S3 object: {e}")
            return BackupItem.ShouldUpload.DoUpload

        if self.trust_mtime:
            # Since we arrived here, the local file had a newer mtime,
            # but the digest was still correct.
            # Bump the locally cached mtime of the S3 object, so we don't
            # need to check the digest until the file is touched
            return BackupItem.ShouldUpload.UpdateModificationTimeOnly
        else:
            return BackupItem.ShouldUpload.DontUpload

    @functools.lru_cache(maxsize=None)
    def stat(self) -> os.stat_result:
        return os.stat(self.path)

    def size(self) -> int:
        return self.stat().st_size

    @functools.lru_cache()
    def digest(self, algorithm: str) -> str:
        digest = hashlib.new(algorithm)
        with self.fileobj() as f:
            while True:
                data = f.read(1024*1024)
                if not data:
                    break
                digest.update(data)
        return f"{{{algorithm.upper()}}}{digest.hexdigest()}"
