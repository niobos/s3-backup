import datetime
import hashlib
import logging
import os
import re
import subprocess
import typing

import boto3

from .s3cache import S3cache, S3ObjectInfo


logger = logging.getLogger(__name__)


def cached(method):
    def check_cache(self, *args, **kwargs):
        if method.__name__ not in self._cache:
            self._cache[method.__name__] = method(self, *args, **kwargs)
        return self._cache[method.__name__]
    return check_cache


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
            logger.log(logging.INFO-2, f"spawning `{xform}`")
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


class IgnoreThisFile(Exception):
    pass


class FileDoesNotExist(Exception):
    pass


class File:
    filename_xform_command = None
    data_xform_command = None
    trust_mtime = True
    storage_class = "STANDARD"

    def __init__(self,
                 base_path: str,
                 relative_path: str,
                 s3_bucket: str,
                 s3_cache: S3cache,
                 s3_client=None,
                 ):
        self.base_path = base_path
        self.relative_path = relative_path
        self.s3_bucket = s3_bucket
        self.s3_cache = s3_cache
        self._s3_client = s3_client

        self._cache = dict()
        self.ignore = False

    @property
    def s3_client(self):
        if self._s3_client is None:
            self._s3_client = boto3.client('s3')
        return self._s3_client

    @property
    def absolute_filename(self) -> str:
        return os.path.join(self.base_path, self.relative_path)

    @property
    @cached
    def s3_key(self) -> str:
        if self.filename_xform_command is None:
            return self.relative_path

        logger.log(logging.INFO-2, f"spawning `{self.filename_xform_command}`")
        xform_env = os.environ.copy()
        xform_env['FILENAME'] = self.relative_path
        xform = subprocess.run(
            ["/bin/bash", "-c", self.filename_xform_command],
            input=self.relative_path,
            encoding='utf-8',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=xform_env,
        )

        if xform.returncode == 124:
            # special case: ignore this file
            self.ignore = True
        if xform.returncode != 0:
            raise OSError(xform.stderr)

        return xform.stdout

    @property
    @cached
    def stat(self) -> os.stat_result:
        return os.stat(self.absolute_filename)

    @property
    def plaintext_size(self) -> int:
        return self.stat.st_size

    def _digest(self, algorithm: str) -> str:
        digest = hashlib.new(algorithm)
        with open(self.absolute_filename, "rb") as f:
            while True:
                data = f.read(1024*1024)
                if not data:
                    break
                digest.update(data)
        return f"{{{algorithm.upper()}}}{digest.hexdigest()}"

    def digest(self, algorithm: str) -> str:
        if algorithm not in self._cache.setdefault('digest', {}):
            self._cache['digest'][algorithm] = self._digest(algorithm)
        return self._cache['digest'][algorithm]

    @property
    def plaintext_hash(self) -> str:
        return self.digest('SHA256')

    def upload_needed(self) -> typing.Union[bool, str]:
        if self.ignore:
            # file is ignored, never upload
            return False
        if self.s3_key == "":
            # the empty key is handled as "pretend file doesn't exist" => never upload
            return False

        try:
            s3_info = self.s3_cache[self.s3_key]
        except KeyError:
            return 'does not exist on S3'  # key not found in S3 cache

        if self.stat.st_size != s3_info.plaintext_size:
            return f"different size ({self.stat.st_size} != {s3_info.plaintext_size}"

        if self.trust_mtime:
            local_mtime = datetime.datetime.fromtimestamp(self.stat.st_mtime)
            if local_mtime < s3_info.s3_modification_time:
                return False  # mtimes are trustworthy, no need to check hash
            # else: verify digest

        try:
            algorithm = re.match(r'^{([^}]+)}', s3_info.plaintext_hash).group(1)
            my_digest = self.digest(algorithm)
            if my_digest != s3_info.plaintext_hash:
                return f"more recent locally & different hash ({my_digest} != {s3_info.plaintext_hash})"
        except (AttributeError,  # regex doesn't match
                TypeError,  # plaintext_hash is None
                ValueError,  # algorithm isn't known
                ) as e:
            return f"could not get plaintext_hash of S3 object: {e}"

        return False

    def do_upload(self, storage_class: str = "STANDARD") -> None:
        with open(self.absolute_filename, "rb") as f:
            f = DataXform(
                self.data_xform_command, f,
                extra_env={
                    'ORIG_FILENAME': self.relative_path,
                    'XFORM_FILENAME': self.s3_key,
                },
            )

            _ = self.s3_client.upload_fileobj(
                Fileobj=f,
                Bucket=self.s3_bucket,
                Key=self.s3_key,
                ExtraArgs={
                    'StorageClass': storage_class,
                    'Metadata': {
                        'plaintext-size': str(self.plaintext_size),
                        'plaintext-hash': self.plaintext_hash,
                    }
                },
            )

        self.s3_cache[self.s3_key] = S3ObjectInfo(
            s3_size=f.size,
            s3_modification_time=datetime.datetime.now(),
            plaintext_size=self.plaintext_size,
            plaintext_hash=self.plaintext_hash,
        )
