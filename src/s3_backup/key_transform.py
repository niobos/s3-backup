import datetime
import functools
import logging
import os
import subprocess
import typing

from s3_backup.backup_item import BackupItem, logger


class KeyTransform(BackupItem):
    def __init__(
            self,
            filename_xform_command: str,
            underlying: BackupItem,
    ):
        self.underlying = underlying
        self.filename_xform_command = filename_xform_command

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {repr(self.underlying)}>"

    def __str__(self) -> str:
        return repr(self)

    @functools.lru_cache(maxsize=None)
    def key(self) -> str:
        logger.log(logging.INFO-2, f"spawning `{self.filename_xform_command}`")
        env = os.environ.copy()
        env['KEY'] = self.underlying.key()
        xform = subprocess.run(
            ["/bin/bash", "-c", self.filename_xform_command],
            input=self.underlying.key(),
            encoding='utf-8',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )

        if xform.returncode != 0:
            raise OSError(xform.stderr)

        return xform.stdout

    def fileobj(self) -> typing.Generator[typing.BinaryIO, None, None]:
        return self.underlying.fileobj()

    def metadata(self) -> typing.Mapping[str, str]:
        return self.underlying.metadata()

    def should_upload(
            self,
            modification_time: typing.Optional[datetime.datetime],
            metadata: typing.Optional[typing.Mapping[str, str]]
    ) -> BackupItem.ShouldUpload:
        return self.underlying.should_upload(
            modification_time=modification_time,
            metadata=metadata,
        )

    @staticmethod
    def wrap_iter(
            it: typing.Iterator["BackupItem"],
            wrapper: typing.Callable[["BackupItem"], "BackupItem"],
    ) -> typing.Generator["BackupItem", None, None]:
        for item in it:
            wrapped_item = wrapper(item)
            if wrapped_item.key() == "":
                continue
            yield wrapped_item
