"""
Functions for getting hashes from bytes.

Usage:

>>> from argendata_datasets.checksum import hash
>>> hash.sha1(b'hello')
>>> hash.sha1('hello'.encode('utf-8'))
"""

from typing import Callable, cast
from ._hashlib import get_hash_method
from ._hash import Hash, HASH_FUNC

def _wrap(name: str, f: Callable):
    def wrapped(*args, **kwargs):
        result = f(*args, **kwargs)
        return Hash(method = name, hexdigest=result.hexdigest())
    return wrapped

def __getattr__(name: str):
    f = get_hash_method(name)
    return cast(HASH_FUNC, _wrap(name, f))