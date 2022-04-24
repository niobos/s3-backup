import enum
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
    def fileobj(self) -> typing.Generator[typing.BinaryIO, None, None]:
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
