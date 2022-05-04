import contextlib
import dataclasses
import datetime
import hashlib
import io
import logging
import typing
import zipfile

import humanize

from s3_backup import BackupItem, global_settings
from s3_backup.backup_item import BackupItemWrapper


logger = logging.getLogger(__name__)


class GroupedItem(BackupItem):
    def __init__(self, key: str, underlying: typing.List[BackupItem], size: int = None):
        self._key = key
        self.underlying_list = underlying
        self.size = size

    def key(self) -> str:
        return self._key

    def __repr__(self) -> str:
        return f"<GroupedItem {self._key}: {self.underlying_list}>"

    @contextlib.contextmanager
    def fileobj(self) -> typing.Generator[typing.BinaryIO, None, None]:
        zip_blob = io.BytesIO()
        with zipfile.ZipFile(zip_blob, "w", compression=zipfile.ZIP_STORED) as zip_file:
            for entry in self.underlying_list:
                mtime = datetime.datetime.fromtimestamp(entry.mtime(), tz=datetime.timezone.utc)
                with entry.fileobj() as entry_fileobj:
                    zip_file.writestr(
                        zipfile.ZipInfo(
                            entry.key(),
                            (mtime.year, mtime.month, mtime.day, mtime.hour, mtime.minute, mtime.second),
                        ),
                        entry_fileobj.read(),
                    )
        zip_blob.seek(0)
        yield zip_blob

    def num_items(self) -> int:
        return len(self.underlying_list)

    def list_hash(self) -> str:
        blob = hashlib.sha256()
        for entry in self.underlying_list:
            blob.update((entry.key() + "\0" + str(entry.size()) + "\0").encode('utf-8'))
        return blob.hexdigest()

    def mtime(self) -> typing.Optional[float]:
        mtime = None
        for entry in self.underlying_list:
            entry_mtime = entry.mtime()
            if mtime is None or entry_mtime > mtime:
                mtime = entry_mtime

        return mtime

    def content_hash(self) -> str:
        blob = hashlib.sha256()
        for entry in self.underlying_list:
            blob.update((entry.key() + "\0" + entry.hash() + "\0").encode('utf-8'))
        return blob.hexdigest()

    def hash(self) -> str:
        return self.content_hash()

    def metadata(self) -> typing.Dict[str, str]:
        return {
            'num-items': str(self.num_items()),
            'list-hash': self.list_hash(),
            'content-hash': self.content_hash(),
        }

    def should_upload(
            self,
            modification_time: typing.Optional[datetime.datetime],
            metadata: typing.Optional[typing.Mapping[str, str]],
    ) -> BackupItem.ShouldUpload:
        if modification_time is None:  # not on S3
            logger.info(f"{self} needs uploading: "
                        f"not on S3")
            return BackupItem.ShouldUpload.DoUpload
        # else:

        if global_settings.trust_mtime:
            if self.mtime() <= modification_time.timestamp():
                return BackupItem.ShouldUpload.DontUpload
            # else: check hashes

        # check metadata. Start with the cheap methods,
        # only run more expensive checks if the cheaper ones don't see a difference
        num_items = str(self.num_items())
        if num_items != metadata.get('num-items', ''):
            logger.info(f"{self} needs uploading: "
                        f"number of items differ {num_items} != {metadata.get('num-items', '')}")
            return BackupItem.ShouldUpload.DoUpload

        list_hash = self.list_hash()
        if list_hash != metadata.get('list-hash', ''):
            logger.info(f"{self} needs uploading: "
                        f"list-hash differs: {list_hash} != {metadata.get('list-hash', '')}")
            return BackupItem.ShouldUpload.DoUpload

        content_hash = self.content_hash()
        if content_hash != metadata.get('content-hash', ''):
            logger.info(f"{self} needs uploading: "
                        f"content-hash differs: {content_hash} != {metadata.get('content-hash', '')}")
            return BackupItem.ShouldUpload.DoUpload

        if global_settings.trust_mtime:
            # Since we arrived here, the local file(s) have a newer mtime,
            # but the digest was still correct.
            # Bump the locally cached mtime of the S3 object, so we don't
            # need to check the digest until the file(s) are touched again
            return BackupItem.ShouldUpload.UpdateModificationTimeOnly
        else:
            return BackupItem.ShouldUpload.DontUpload


class Tree:
    @dataclasses.dataclass
    class Element:
        key: str
        size: int
        item: BackupItem

        def __lt__(self, other):
            return self.key < other.key

    def __init__(self):
        self.elements: typing.List[Tree.Element] = []
        self.key_prefixes: typing.Dict[str, "Tree"] = {}

    def add_elements(self, elements: typing.Iterable[Element]):
        for el in elements:
            self.add_element(el)

    def add_element(self, element: Element):
        key = element.key
        if len(key) <= 1:
            self.elements.append(element)
        else:
            first_char = key[0]
            if first_char not in self.key_prefixes:
                self.key_prefixes[first_char] = Tree()
            self.key_prefixes[first_char].add_element(Tree.Element(
                key=element.key[1:],
                size=element.size,
                item=element.item,
            ))

    def elements_size(self) -> int:
        size = 0
        for element in self.elements:
            size += element.size
        return size

    def children_size(self) -> int:
        size = 0
        for node in self.key_prefixes.values():
            size += node.size()
        return size

    def size(self) -> int:
        return self.children_size() + self.elements_size()

    def flatten(self):
        for key_prefix in list(self.key_prefixes.keys()):
            node = self.key_prefixes[key_prefix]
            node.flatten()

            if len(node.key_prefixes) == 1 and len(node.elements) == 0:
                only_prefix = next(iter(node.key_prefixes.keys()))
                self.key_prefixes[key_prefix + only_prefix] = node.key_prefixes[only_prefix]
                del self.key_prefixes[key_prefix]

    def merge_min_size(self, min_size: int):
        for key_prefix in list(self.key_prefixes.keys()):
            node = self.key_prefixes[key_prefix]
            node.merge_min_size(min_size)

            if len(node.elements) and node.elements_size() < min_size:
                # Elements in the node are too small, pull them up to this level
                self.elements.extend((
                    Tree.Element(key=key_prefix + el.key, size=el.size, item=el.item)
                    for el in node.elements
                ))
                node.elements = []

            if len(node.elements) == 0 and len(node.key_prefixes) == 0:
                # Remove empty nodes
                del self.key_prefixes[key_prefix]


def group_files(items: typing.Iterator[BackupItem], min_size: int) -> typing.Iterator[GroupedItem]:
    tree = Tree()
    tree.add_elements((
        Tree.Element(item.key(), item.size(), item)
        for item in items
    ))
    tree.flatten()  # reduce recursion
    tree.merge_min_size(min_size)
    tree.flatten()  # again, since we pulled up elements in the merge_min_size()-step

    def recurse_tree(node: Tree, prefix: str = ''):
        for child_prefix, child in node.key_prefixes.items():
            for item in recurse_tree(child, prefix + child_prefix):
                yield item

        if len(node.elements) > 0:
            yield GroupedItem(
                key=prefix,
                underlying=[el.item for el in node.elements],
                size=node.elements_size(),
            )

    return recurse_tree(tree)


class GroupSmallFilesWrapper(BackupItemWrapper):
    def __init__(
            self,
            underlying_it: typing.Iterator[BackupItem],
            size_threshold: int,
    ):
        super().__init__(underlying_it)
        self.size_threshold = size_threshold
        self.suffix = '*~.zip'
        self.num_zip_files = 0
        self.min_size = [None, None]  # Keep the 2 smallest files
        # The "leftover" zip will always be smallest, but is not representative
        self.max_size = None
        self.small_files = 0
        self.small_files_bytes = 0
        self.large_files = 0
        self.large_files_bytes = 0

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(size_threshold={self.size_threshold})"

    def summary(self) -> str:
        return f"Generated {self.num_zip_files} ZIPs, ranging from " \
               f"{humanize.naturalsize(self.min_size[1], binary=True)} to " \
               f"{humanize.naturalsize(self.max_size, binary=True)} each " \
               f"({humanize.naturalsize(self.min_size[0], binary=True)} for the leftover ZIP)\n" \
               f"together containing {self.small_files} files, " \
               f"{humanize.naturalsize(self.small_files_bytes, binary=True)}, " \
               f"each <{humanize.naturalsize(self.size_threshold, binary=True)}\n" \
               f"({self.large_files} files, {humanize.naturalsize(self.large_files_bytes, binary=True)}, " \
               f"each >={humanize.naturalsize(self.size_threshold, binary=True)}, passed through)"

    def __iter__(self) -> typing.Generator[BackupItem, None, None]:
        small_files = []
        for entry in self.underlying_it:
            if entry.key().endswith(self.suffix):
                raise RuntimeError(f"Got key `entry.key()` that collides with "
                                   f"{self.__class__.__name__} configured suffix of `{self.suffix}`")

            size = entry.size()
            if size is not None and size < self.size_threshold:
                self.small_files += 1
                self.small_files_bytes += size
                small_files.append(entry)
            else:
                self.large_files += 1
                self.large_files_bytes += size
                yield entry

        for entry in group_files(small_files, min_size=self.size_threshold):
            entry._key = entry._key + self.suffix

            logger.log(logging.INFO-1,
                       f"`{entry.key()}`  "
                       f"({len(entry.underlying_list)} entries, "
                       f"{humanize.naturalsize(entry.size, binary=True)})"
                       "\n    " + "\n    ".join([str(_) for _ in entry.underlying_list]))
            yield entry

            self.num_zip_files += 1

            if self.min_size[0] is None or entry.size < self.min_size[0]:
                self.min_size[1] = self.min_size[0]
                self.min_size[0] = entry.size
            elif self.min_size[1] is None or entry.size < self.min_size[1]:
                self.min_size[1] = entry.size

            if self.max_size is None or entry.size > self.max_size:
                self.max_size = entry.size
