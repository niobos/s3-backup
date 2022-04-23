import contextlib
import datetime
import logging
import os
import subprocess
import typing

from s3_backup.backup_item import BackupItem, logger


class DataTransform(BackupItem):
    METADATA_PREFIX = "plaintext-"

    def __init__(
            self,
            data_xform_command: str,
            underlying: BackupItem,
    ):
        self.data_xform_command = data_xform_command
        self.underlying = underlying

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {repr(self.underlying)}>"

    def __str__(self) -> str:
        return repr(self)

    def key(self) -> str:
        return self.underlying.key()

    class DataXformReadWrapper:
        """
        Helper class to Upload an S3 object while its content is generated on
        the fly.

        s3_client.upload_fileobj() will read() from the given object when it needs
        more data. This class will read from the transform-subprocess.
        """
        def __init__(self,
                     xform: str,
                     fileobj: typing.BinaryIO,
                     extra_env: dict = None
                     ):
            xform_env = os.environ.copy()
            if extra_env is not None:
                xform_env.update(extra_env)

            logger.log(logging.INFO-2, f"spawning `{xform}`")
            self.subprocess = subprocess.Popen(
                ["/bin/bash", "-c", xform],
                stdin=fileobj,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=xform_env,
            )
            self.output = self.subprocess.stdout

            self.size = 0

        def read(self, size=-1) -> bytes:
            data = self.output.read(size)

            self.size += len(data)

            if len(data) == 0 and self.subprocess is not None:
                return_code = self.subprocess.wait()
                if return_code != 0:
                    raise OSError(self.subprocess.stderr)

            return data

    @contextlib.contextmanager
    def fileobj(self) -> typing.Generator[typing.BinaryIO, None, None]:
        with self.underlying.fileobj() as f_orig:
            f_wrapped = DataTransform.DataXformReadWrapper(
                self.data_xform_command, f_orig,
                extra_env={
                    'KEY': self.key(),
                },
            )
            yield f_wrapped

    def metadata(self) -> typing.Mapping[str, str]:
        m = {
            f"{self.METADATA_PREFIX}{key}": value
            for key, value in self.underlying.metadata().items()
        }
        return m

    def should_upload(
            self,
            modification_time: typing.Optional[datetime.datetime],
            metadata: typing.Optional[typing.Mapping[str, str]],
    ) -> BackupItem.ShouldUpload:
        if modification_time is None:  # not on S3
            return self.underlying.should_upload(None, None)
        # else:

        underlying_metadata = {
            key[len(self.METADATA_PREFIX):]: value
            for key, value in metadata.items()
            if key.startswith(self.METADATA_PREFIX)
        }

        return self.underlying.should_upload(
            modification_time=modification_time,
            metadata=underlying_metadata,
        )
