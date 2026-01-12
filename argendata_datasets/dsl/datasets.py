from typing import TYPE_CHECKING, Callable
from .singleton import Singleton
import pathlib

class Dataset: ...

type DatasetGetter = Callable[[str], Dataset]
type DatasetDownloader = Callable[[str, pathlib.Path], pathlib.Path]


class DatasetProxy:
    __registrations_metadata__: list[dict]

    def __init__(self, name: str, client: 'Client'):
        self.name = name
        self.__registrations_metadata__ = list()
        self.client = client

    def get(
        self, *,
        version: None|str = None,
        by: None|DatasetGetter = None
    ) -> str:
        uri = f'{self.name}' + ('' if not version else f'@{version}')
        Client().uses(uri)

        if by:
            return by(uri=uri)

        return uri
    
    def download(
        self, *,
        to: str|pathlib.Path,
        version: None|str = None,
        by: None|DatasetDownloader = None,
    ) -> pathlib.Path:
        uri = f'{self.name}' + ('' if not version else f'@{version}')
        Client().uses(uri)

        to = pathlib.Path(to) if isinstance(to, str) else to

        if by:
            return by(uri=uri, to=to)

        return to / uri

    def register(self, *, filename: str, **extra) -> None:

        self.__registrations_metadata__.append({
            "name": self.name,
            "filename": filename,
            **extra
        })

        self.client._produced.append({
            "name": self.name,
            "filename": filename,
            **extra
        })

        print(f"Registered dataset {self.name} with filename {filename} and metadata {extra}")
        return self
    
    def _get_registrations_metadata_stack(self) -> dict:
        return self.client._produced

    def save(self, /, obj, func=None, **kwargs):
        if func is None:
            return obj
        
        return func(obj, **kwargs)

class MetadataClient(metaclass=Singleton):
    def __init__(self):
        ...
    
    def get(self, *fields, by: None|Callable = None) -> dict:
        if by:
            return by(*fields)
        
        return dict()

    def __getattr__(self, name: str): 
        return MetadataClient()


class Client(metaclass=Singleton):
    _used: set[str]
    _produced: list[dict]
    metadata: MetadataClient

    def __init__(self):
        self._used = set()
        self._produced = list()
        self.metadata = MetadataClient()

    @property
    def used(self) -> frozenset[str]:
        return frozenset(self._used)

    @property
    def produced(self) -> list[dict]:
        return self._produced

    def uses(self, uri: str):
        self._used.add(uri)

    def __getattr__(self, name: str): 
        return DatasetProxy(name, self)

# ==============================================================================
# Dummy specifications for type checking purposes.

if TYPE_CHECKING:
    class DatasetProxy(type):
        def __init__(self, name: str): ...

        @classmethod
        def get(
            self, *, 
            version: None|str = None, 
            by: None|DatasetGetter = None) -> str:
            """
            Args:
                - version: The version of the dataset to get.
                - by: A function that takes a parameter 'uri: str' and returns a Dataset. If present, will be used to retrieve the Dataset without guarantees.
            """
            ...

        @classmethod
        def download(
            self, *, 
            to: str|pathlib.Path,
            version: None|str = None,
            by: None|DatasetDownloader = None
            ) -> pathlib.Path:
            """
            Args:
                - to: The path to download the dataset to.
                - version: The version of the dataset to download.
                - by: A function that takes a parameter 'uri: str' and a parameter 'to: pathlib.Path' and returns a pathlib.Path. If present, will be used to download the Dataset without guarantees.
            """
            ...

    class Client(type):
        used: set[str]
        metadata: MetadataClient
        """
        The list of unique datasets that have been used.
        """

        def __init__(self): ...
        def __getattr__(self, name: str) -> type[DatasetProxy]: ...

    class MetadataClient(type):
        def __init__(self): ...
        def get(self, /, *fields, by: None|Callable = None) -> dict: ...
        def __getattr__(self, name: str) -> DatasetProxy: ...

Datasets = Client()