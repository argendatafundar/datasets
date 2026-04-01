from pathlib import Path
from typing import Protocol
from pydantic import BaseModel

class DatasetGetter(Protocol):
    def __call__(self, dataset_id: str, version: str):
        ...

class DatasetDownloader(Protocol):
    def __call__(self, dataset_id: str, version: str, to: str|Path):
        ...

def _default_get(id: str):
    from argendata_api import Client
    import re
    client = Client.default()
    client.login()
    pattern = re.compile('R([0-9]+)C([0-9]+)')
    matches = pattern.match(id)

    if not matches:
        raise ValueError(f"Invalid id '{id}'")
    
    r = matches.group(1)
    c = matches.group(2)

    datasets = client.datasets
    target = datasets.clean if int(c) > 0 else datasets.raw

    response = target.inspect(id)
    response.raise_for_status()

    return response.content
    

class Proxy:
    def __init__(self, dataset_id: str, parent: 'Datasets') -> None:
        self.dataset_id = dataset_id
        self.parent = parent

    def get(self, by: None|DatasetGetter=None):
        self.parent.DEPENDENCIES.add(self.dataset_id)

        if not by:
            return _default_get(self.dataset_id)
        
        return by(self.dataset_id)
    
    def download(self, to: str|Path, by: None|DatasetDownloader=None):
        self.parent.DEPENDENCIES.add(self.dataset_id)

    def register(self, filepath: str, **kwargs):
        data = dict(filepath=filepath, **kwargs)
        self.parent.REGISTRY[self.dataset_id] = data
    
    def save(self, /, obj, func=None, **kwargs):
        self.parent.EXPORTS.add(self.dataset_id)

        if func is None:
            return obj 

        return func(obj, **kwargs)

class Datasets(type):
    DEPENDENCIES = set()
    REGISTRY = dict()
    EXPORTS = set()
    
    @staticmethod
    def use(name: str):
        return Proxy(name, Datasets)
    
    def require(self, name: str):
        return type(self).use(name)

    def __getattr__(self, name: str):
        return self.require(name)
    
    class _Representation(BaseModel):
        dependencies: list[str]
        registry: dict[str, dict[str, object]]
        exports: list[str]

    @classmethod
    def get_representation(cls):
        return cls._Representation(
            dependencies=list(cls.DEPENDENCIES),
            registry=cls.REGISTRY,
            exports=list(cls.EXPORTS)
        )
    
    @classmethod
    def model_dump(cls, **kwargs):
        return cls.get_representation().model_dump(**kwargs)
    
    @classmethod
    def model_dump_json(cls, **kwargs):
        return cls.get_representation().model_dump_json(**kwargs)

class Dataset(metaclass=Datasets): ...