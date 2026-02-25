import numbers

import humanize


class Stats:
    def __init__(self):
        self.upload_files = 0
        self.upload_bytes = 0
        self.delete_files = 0

    def upload(self, size: int) -> None:
        self.upload_files += 1
        self.upload_bytes += size

    def delete(self) -> None:
        self.delete_files += 1

    def summary(self) -> str:
        return f"""\
        Uploaded {self.upload_files} files, totalling {humanize.naturalsize(self.upload_bytes, binary=True)}
        Deleted {self.delete_files} files
        """


class MinMax:
    def __init__(self, min_amount: int = 1, max_amount: int = 1) -> None:
        self.minimum = []
        self.maximum = []
        self._min_amount = min_amount
        self._max_amount = max_amount

    def update(self, n: numbers.Number) -> None:
        if self._max_amount > 0:
            self.maximum = sorted([*self.maximum, n])[-self._max_amount:]
        if self._min_amount > 0:
            self.minimum = sorted([*self.minimum, n])[:self._min_amount]

    def __repr__(self):
        return f"MinMax({self.minimum}, {self.maximum})"
