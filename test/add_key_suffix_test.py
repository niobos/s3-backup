from s3_backup.add_key_suffix import AddKeySuffixWrapper


class MockItem:
    def __init__(self, key: str):
        self._key = key

    def key(self) -> str:
        return self._key


def test_add_key_suffix():
    items = [
        MockItem("foo"),
        MockItem("bar"),
    ]
    renamed_items = list(AddKeySuffixWrapper(items, '.test'))
    assert renamed_items[0].key() == "foo.test"
    assert renamed_items[1].key() == "bar.test"
    assert len(renamed_items) == 2
