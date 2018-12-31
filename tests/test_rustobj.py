import pytest

from sourmash.utils import RustObject
from sourmash._minhash import to_bytes


def test_rustobj_init():
    with pytest.raises(TypeError):
        RustObject()


def test_to_bytes():
    with pytest.raises(TypeError):
        to_bytes(0)
