import humanize


class Stats:
    def __init__(self):
        self.scanned_files = 0
        self.scanned_bytes = 0
        self.upload_files = 0
        self.upload_bytes = 0
        self.delete_files = 0

    def scan(self, size: int) -> None:
        self.scanned_files += 1
        self.scanned_bytes += size

    def upload(self, size: int) -> None:
        self.upload_files += 1
        self.upload_bytes += size

    def delete(self) -> None:
        self.delete_files += 1

    def summary(self) -> str:
        return f"""\
        Scanned {self.scanned_files} files, totalling {humanize.naturalsize(self.scanned_bytes, binary=True)}
        Uploaded {self.upload_files} files, totalling {humanize.naturalsize(self.upload_bytes, binary=True)}
        Deleted {self.delete_files} files
        """
