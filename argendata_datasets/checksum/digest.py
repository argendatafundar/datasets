"""
Functions for loading files and buffers and computing their checksums.

Usage:

>>> from argendata_datasets.checksum import digest
>>> digest.sha1('file.txt')
>>> digest.sha1(io.BytesIO(b'hello'))
"""

from typing import Callable, Concatenate, cast
from ._hash import Hash
from ._hashlib import get_hash_method
import pathlib
from typing import BinaryIO

PathLike = str | pathlib.Path

type DIGEST_FUNC[**P] = Callable[
    Concatenate[PathLike|BinaryIO, bool, P],
    Hash
]

def _wrap(name: str, f: Callable):
    def wrapped(x: PathLike|BinaryIO, *args, **kwargs):
        if not isinstance(x, PathLike):
            # BinaryIO
            value = x.read()
            result = f(value, *args, **kwargs)
            return Hash(
                method = name,
                hexdigest = result.hexdigest(),
                filename = None,
            )
        else:
            x = pathlib.Path(x)
            result = f(x.read_bytes(), *args, **kwargs)
            return Hash(
                method = name,
                hexdigest = result.hexdigest(),
                filename = str(x),
            )
        
        
    return wrapped

def __getattr__(name: str):
    f = get_hash_method(name)
    return cast(DIGEST_FUNC, _wrap(name, f))