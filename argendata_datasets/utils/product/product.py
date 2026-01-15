from dataclasses import dataclass
from argendata_datasets import checksum
from . import patterns
import re

@dataclass
class Product:
    codigo: None|str
    checksum: checksum.Hash

    @classmethod
    def from_str(cls, s: str):
        match = re.match(patterns.PRODUCT, s)
        
        if match:
            codigo, hash_str = match.groups()
            hash = checksum.Hash.from_str(hash_str)
            return cls(codigo=codigo, checksum=hash)
        
        match = re.match(patterns.HASH, s)
        if match:
            hash = checksum.Hash.from_str(s)
            return cls(codigo=None, checksum=checksum.Hash.from_str(s))
        
        raise ValueError(f"Invalid product: {s}")