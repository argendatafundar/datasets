from pathlib import Path
from typing import Annotated, Protocol
from pydantic import BaseModel
from enum import StrEnum
import os

__external_runtime_environment_variable__ = 'ARGENDATA_EXTERNAL_RUNTIME'

def get_external_runtime():
    env_var = os.environ.get(__external_runtime_environment_variable__, None)
    
    if not env_var:
        return False
    
    return env_var.lower() in ('true', '1')

EXTERNAL_RUNTIME: bool = get_external_runtime()

class DatasetGetter(Protocol):
    def __call__(self, dataset_id: str, version: str):
        ...

class DatasetDownloader(Protocol):
    def __call__(self, dataset_id: str, version: str, to: str|Path):
        ...

def _default_get(id: str):
    from argendata_internal_client import Client, Settings
    import re
    client = Client.default()
    print(client)
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

class Categoria(StrEnum):
    AMBIENTE      = 'Ambiente'
    MACROECONOMIA = 'Macroeconomía'
    DESARROLLO    = 'Desarrollo'
    PRODUCTIVOS   = 'Sectores productivos'
    TRABAJO       = 'Trabajo e ingresos'
    POBLACION     = 'Población'

class Topicos(StrEnum):
    ACECON: Annotated[str, None,                    'Actividad Económica']                   = 'ACECON'
    AGROPE: Annotated[str, Categoria.PRODUCTIVOS,   'Agroindustria']                         = 'AGROPE'
    CIETEC: Annotated[str, Categoria.DESARROLLO,    'Ciencia y tecnología']                  = 'CIETEC'
    COMEXT: Annotated[str, Categoria.DESARROLLO,    'Comercio exterior']                     = 'COMEXT'
    CRECIM: Annotated[str, Categoria.MACROECONOMIA, 'Crecimiento']                           = 'CRECIM'
    DEMOGR: Annotated[str, Categoria.POBLACION,     'Demografía']                            = 'DEMOGR'
    DESHUM: Annotated[str, Categoria.DESARROLLO,    'Desarrollo humano']                     = 'DESHUM'
    DESIGU: Annotated[str, Categoria.DESARROLLO,    'Desigualdad']                           = 'DESIGU'
    ESTPRO: Annotated[str, Categoria.PRODUCTIVOS,   'Estructura productiva']                 = 'ESTPRO'
    FISCAL: Annotated[str, Categoria.MACROECONOMIA, 'Fiscal']                                = 'FISCAL'
    INDUST: Annotated[str, Categoria.PRODUCTIVOS,   'Industria']                             = 'INDUST'
    INFDES: Annotated[str, Categoria.TRABAJO,       'Informalidad y desempleo']              = 'INFDES'
    MERTRA: Annotated[str, Categoria.TRABAJO,       'Mercado laboral']                       = 'MERTRA'
    MINERI: Annotated[str, Categoria.PRODUCTIVOS,   'Minería']                               = 'MINERI'
    PESCAS: Annotated[str, Categoria.PRODUCTIVOS,   'Pesca y acuicultura']                   = 'PESCAS'
    POBREZ: Annotated[str, Categoria.DESARROLLO,    'Pobreza']                               = 'POBREZ'
    PRECIO: Annotated[str, Categoria.MACROECONOMIA, 'Precio']                                = 'PRECIO'
    SALING: Annotated[str, Categoria.TRABAJO,       'Salarios e ingresos']                   = 'SALING'
    SCROLL: Annotated[str, Categoria.PRODUCTIVOS,   'Scroll']                                = 'SCROLL'
    SEBACO: Annotated[str, Categoria.PRODUCTIVOS,   'Servicios basados en el conocimiento']  = 'SEBACO'
    TRANEN: Annotated[str, Categoria.AMBIENTE,      'Transición energética']                 = 'TRANEN'
    TURISM: Annotated[str, Categoria.PRODUCTIVOS,   'Turismo']                               = 'TURISM'
    CAMCLI: Annotated[str, Categoria.AMBIENTE,      'Cambio climático y emisiones de gases de efecto invernadero'] = 'CAMCLI'
