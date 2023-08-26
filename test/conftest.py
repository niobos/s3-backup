import os

import pytest


@pytest.fixture(params=["file.ext", "dir/file.ext", "special\" \'/\n"])
def filepath(request):
    """
    Test different module addresses.
    Especially test the case where decimal & hex representations differ,
    and test leading 0's
    """
    return request.param


@pytest.fixture(params=[
    ('regular-filename.txt', b"ascii content"),
    ('special "filename\'\n.bin', b"\x00\x01\x02\x03\x04\x05\x80"),
])
def testfile(request, tmp_path):
    """
    Create a file with some content in.
    Returns a tuple (filename, content)
    """
    filename = f"{tmp_path}/{request.param[0]}"
    content = request.param[1]
    with open(filename, "wb") as f:
        f.write(content)

    yield filename, content

    os.unlink(filename)
