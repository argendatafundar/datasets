from pathlib import Path
from typing import Protocol

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
    matches = re.match(pattern, id)

    if not matches:
        raise ValueError('Invalid id')
    
    r = matches.group(1)
    c = matches.group(2)

    datasets = client.datasets
    target = datasets.clean if int(c) > 0 else datasets.raw

    return target.get(id)
    

class Proxy:
    def __init__(self, dataset_id: str, parent: 'Datasets') -> None:
        self.dataset_id = dataset_id
        self.parent = parent

    def get(self, by: None|DatasetGetter=None):
        self.parent.DEPENDENCIES.add((self.dataset_id, by))

        if not by:
            return _default_get(self.dataset_id)
        
        return by(self.dataset_id)
    
    def download(self, to: str|Path, by: None|DatasetDownloader=None):
        self.parent.DEPENDENCIES.add((self.dataset_id, by))

    def register(self, filename: str, **kwargs):
        data = dict(filename=filename, **kwargs)
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

class Dataset(metaclass=Datasets): ...