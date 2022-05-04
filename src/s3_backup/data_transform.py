import contextlib
import datetime
import io
import logging
import os
import subprocess
import threading
import typing

from s3_backup.backup_item import BackupItem, logger, BackupItemWrapper


def stdin_pump(data: io.BytesIO, fd: typing.BinaryIO) -> None:
    while True:
        blob = data.read(4096)
        if len(blob) == 0:
            fd.close()
            break
        fd.write(blob)


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

        self.stdin_pump = None
        orig_fileobj = None
        if isinstance(fileobj, io.BytesIO):
            orig_fileobj = fileobj
            fileobj = subprocess.PIPE

        logger.log(logging.INFO-2, f"spawning `{xform}`")
        self.subprocess = subprocess.Popen(
            ["/bin/bash", "-c", xform],
            stdin=fileobj,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=xform_env,
        )
        self.output = self.subprocess.stdout

        if orig_fileobj is not None:
            self.stdin_pump = threading.Thread(target=stdin_pump, args=(orig_fileobj, self.subprocess.stdin))
            self.stdin_pump.start()

        self.size = 0

    def read(self, size=-1) -> bytes:
        data = self.output.read(size)
        self.size += len(data)
        return data

    def close(self):
        if self.subprocess is not None:
            return_code = self.subprocess.wait()
            if return_code != 0:
                raise OSError(f"exit code {return_code}\n" + self.subprocess.stderr.read().decode('utf-8'))
            self.subprocess = None
        if self.stdin_pump is not None:
            self.stdin_pump.join()
            self.stdin_pump = None


class DataTransform(BackupItem):
    METADATA_PREFIX = "plaintext-"

    def __init__(
            self,
            data_xform_command: str,
            underlying: BackupItem,
    ):
        self.xform_command = data_xform_command
        self.underlying = underlying

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} xform={self.xform_command} {repr(self.underlying)}>"

    def __str__(self) -> str:
        return repr(self)

    def key(self) -> str:
        return self.underlying.key()

    @contextlib.contextmanager
    def fileobj(self) -> typing.Generator[typing.BinaryIO, None, None]:
        with self.underlying.fileobj() as f_orig:
            f_wrapped = DataXformReadWrapper(
                self.xform_command, f_orig,
                extra_env={
                    'KEY': self.key(),
                },
            )
            yield f_wrapped
            f_wrapped.close()

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

    def hash(self) -> str:
        return self.underlying.hash()

    def mtime(self) -> typing.Optional[float]:
        return self.underlying.mtime()


class DataTransformWrapper(BackupItemWrapper):
    def __init__(
            self,
            underlying_it: typing.Iterator[BackupItem],
            xform_command: str,
    ):
        super().__init__(underlying_it)
        self.xform_command = xform_command

    def __iter__(self) -> typing.Generator[DataTransform, None, None]:
        for item in self.underlying_it:
            wrapped_item = DataTransform(self.xform_command, item)
            yield wrapped_item
