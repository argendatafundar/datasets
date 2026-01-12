from typing import Callable, NamedTuple, Concatenate
from collections.abc import Buffer as ReadableBuffer
from dataclasses import dataclass

type HASH_FUNC[**P] = Callable[
    Concatenate[ReadableBuffer, bool, P],
    Hash
]

def valid_filename(filename: object):
    if filename is None:
        return None

    if not isinstance(filename, str):
        T = type(filename)
        raise TypeError(f"Filename must be of type 'str', got '{T.__name__}'.")
    
    if '@' in filename:
        raise ValueError(f"Filename cannot contain '@': '{filename}'")
    
    return filename

@dataclass
class Hash:
    method: str
    hexdigest: str
    filename: None|str = None

    def __post_init__(self):
        self.filename = valid_filename(self.filename)

    def to_str(self, include_filename: bool = True):
        hash_part = f"{self.method}:{self.hexdigest}"

        if (self.filename is None) or (not include_filename):
            return hash_part
        
        return f"{self.filename}@{hash_part}"
    
    def __str__(self): return self.to_str()

    @classmethod
    def from_str(cls, s: str):
        parts = s.split('@')

        if len(parts) == 2:
            filename, hash_part = parts
        else:
            filename, hash_part = None, parts[0]

        method, hexdigest = hash_part.split(':')
        return cls(method=method, hexdigest=hexdigest, filename=filename)
    
    def __repr__(self):
        return f'<Hash object {self.method}:{self.hexdigest} @ {hex(id(self))}>'

    def __eq__(self, other: object) -> bool:
        if isinstance(other, str):
            return self == Hash.from_str(other)
        
        if not isinstance(other, Hash):
            return False
        
        filename_eq = True
        if (self.filename is not None) and (other.filename is not None):
            filename_eq = self.filename == other.filename
        
        return self.method == other.method and self.hexdigest == other.hexdigest and filename_eq