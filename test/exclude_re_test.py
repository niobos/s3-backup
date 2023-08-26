from s3_backup.exclude_re import ExcludeReWrapper


class MockItem:
    def __init__(self, key: str):
        self._key = key

    def key(self) -> str:
        return self._key


def test_exclude_re():
    items = [
        MockItem("foo"),
        MockItem("bar"),
    ]
    remaining_items = list(ExcludeReWrapper(items, 'f.*'))
    assert remaining_items == [items[1]]
