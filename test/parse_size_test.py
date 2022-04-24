import pytest

from s3_backup.__main__ import parse_size


@pytest.mark.parametrize(
    ['s', 'expected'],
    [
        ('0', 0),
        ('17', 17),
        ('102 B', 102),
        ('1 k', 1000),
        ('1 KB', 1000),
        ('1 KiB', 1024),
        ('2 MiB', 2*1024*1024),
        ('5 Gi', 5*1024*1024*1024)
    ]
)
def test_parse_size(s, expected):
    result = parse_size(s)
    assert result == expected
