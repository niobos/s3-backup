import datetime
import functools
import logging
import os
import re
import subprocess
import typing

from s3_backup.backup_item import BackupItem, logger, BackupItemWrapper


class KeyTransformCmd(BackupItem):
    def __init__(
            self,
            xform_command: str,
            underlying: BackupItem,
    ):
        self.underlying = underlying
        self.xform_command = xform_command

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} xform={self.xform_command} {repr(self.underlying)}>"

    def __str__(self) -> str:
        return repr(self)

    @functools.lru_cache(maxsize=None)
    def key(self) -> str:
        logger.log(logging.INFO - 2, f"spawning `{self.xform_command}` to transform `{self.underlying.key()}`")
        env = os.environ.copy()
        env['KEY'] = self.underlying.key()
        xform = subprocess.run(
            ["/bin/bash", "-c", self.xform_command],
            input=self.underlying.key(),
            encoding='utf-8',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )

        if xform.returncode != 0:
            raise OSError(xform.stderr)

        new_key = xform.stdout
        logger.log(logging.INFO - 2, f"New key: {new_key}")
        return new_key

    def size(self) -> typing.Optional[int]:
        return self.underlying.size()

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
            xform_command: str,
    ) -> typing.Generator["BackupItem", None, None]:
        for item in it:
            wrapped_item = KeyTransformCmd(xform_command, item)
            if wrapped_item.key() == "":
                continue
            yield wrapped_item


class KeyTransformCmdWrapper(BackupItemWrapper):
    def __init__(
            self,
            underlying_it: typing.Iterator[BackupItem],
            xform_command: str,
    ):
        super().__init__(underlying_it)
        self.xform_command = xform_command
        self.skipped = 0

    def summary(self) -> str:
        return f"Skipped {self.skipped} files"

    def __iter__(self) -> typing.Generator[KeyTransformCmd, None, None]:
        for item in self.underlying_it:
            wrapped_item = KeyTransformCmd(self.xform_command, item)
            if wrapped_item.key() == "":
                self.skipped += 1
                continue
            yield wrapped_item


class KeyTransformSub(BackupItem):
    def __init__(
            self,
            underlying: BackupItem,
            sub_pattern: re.Pattern,
            sub_replacement: str,
    ):
        self.underlying = underlying
        self._key = re.sub(sub_pattern, sub_replacement, underlying.key(), count=1)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} key={self._key} {repr(self.underlying)}>"

    def __str__(self) -> str:
        return repr(self)

    def key(self) -> str:
        return self._key

    def size(self) -> typing.Optional[int]:
        return self.underlying.size()

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
            xform_command: str,
    ) -> typing.Generator["BackupItem", None, None]:
        for item in it:
            wrapped_item = KeyTransformCmd(xform_command, item)
            if wrapped_item.key() == "":
                continue
            yield wrapped_item


class KeyTransformSubWrapper(BackupItemWrapper):
    def __init__(
            self,
            underlying_it: typing.Iterator[BackupItem],
            sub_pattern: str, sub_replacement: str,
    ):
        super().__init__(underlying_it)
        self.sub_pattern = re.compile(sub_pattern)
        self.sub_replacement = sub_replacement
        self.passed = 0
        self.renamed = 0
        self.skipped = 0

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.sub_pattern} -> {repr(self.sub_replacement)})"

    def summary(self) -> str:
        return f"Passed {self.passed}, renamed {self.renamed}, skipped {self.skipped} files"

    def __iter__(self) -> typing.Generator[KeyTransformCmd, None, None]:
        for item in self.underlying_it:
            wrapped_item = KeyTransformSub(item, self.sub_pattern, self.sub_replacement)
            if wrapped_item.key() == "":
                self.skipped += 1
                continue

            if wrapped_item.key() == item.key():
                # Wrapping would change nothing, save some memory & overhead by returning the original
                self.passed += 1
                yield item
            else:
                self.renamed += 1
                yield wrapped_item
