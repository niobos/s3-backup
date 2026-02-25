import contextlib
import enum
import io
import logging
import typing
import abc
import datetime


logger = logging.getLogger(__name__)


class BackupItem(abc.ABC):
    @abc.abstractmethod
    def key(self) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def fileobj(self) -> contextlib.AbstractContextManager[io.Reader]:
        raise NotImplementedError()

    class SizeMetadata(str):
        pass

    def metadata(self) -> typing.Dict[str, str]:
        """
        Return additional metadata to store on the object. This data
        can later be used in should_upload() to make the decision.

        The name "size" is treated specially: if it contains a value
        of type SizeMetadata, the value will not be saved explicitly,
        but the S3 built-in Content-Length will be used instead.
        Note that the returned value may differ from the outputted value
        if it is not the actual size.
        """
        return {}

    class ShouldUpload(enum.Enum):
        DontUpload = enum.auto()
        DoUpload = enum.auto()
        UpdateModificationTimeOnly = enum.auto()

    @abc.abstractmethod
    def should_upload(
            self,
            modification_time: typing.Optional[datetime.datetime],
            metadata: typing.Optional[typing.Mapping[str, str]],
    ) -> ShouldUpload:
        """
        Verify if this BackupItem should be uploaded again based on the
        `modification_time` and/or `metadata` of the current object on S3,
        or None if it's not on S3.
        The `metadata` is the data you provided in the .metadata() method on the
        previous upload.
        """
        raise NotImplementedError()

    def size(self) -> typing.Optional[int]:
        """
        If the size of the file is known, return it.
        Otherwise, return None
        """
        return None

    @abc.abstractmethod
    def hash(self) -> str:
        """
        Returns a hash of the item.
        If an item hashes to the same value, it is assumed to not have changed.
        """
        raise NotImplementedError()

    def mtime(self) -> typing.Optional[float]:
        """
        Return a modification time, if available.
        Seconds since unix epoch
        """
        return None


class BackupItemWrapper(abc.ABC):
    def __init__(
            self,
            underlying_it: typing.Iterator[BackupItem],
    ):
        self.underlying_it = underlying_it

    def summary(self) -> str:
        return ""

    @abc.abstractmethod
    def __iter__(self) -> typing.Generator[BackupItem, None, None]:
        raise NotImplementedError()
