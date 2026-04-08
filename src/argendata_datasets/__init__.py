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

def _version_id_from_inspect_json(data: dict) -> str:
    """Latest version id from a blob inspect payload, or the single version from a pinned inspect."""
    if 'versions' in data:
        vers = data['versions']
        if not vers:
            raise ValueError('Dataset inspect returned no versions')
        return vers[-1]['version_id']
    vid = data.get('version_id')
    if vid is None:
        raise ValueError('Dataset inspect JSON missing version_id')
    return vid


def _default_get(dataset_id: str, version: str):
    """Fetch dataset bytes via HTTP; records resolved version in ``Datasets.DEPENDENCIES``."""
    from argendata_internal_client import Client
    import re

    client = Client.default()

    pattern = re.compile('R([0-9]+)C([0-9]+)')
    matches = pattern.match(dataset_id)

    if not matches:
        raise ValueError(f"Invalid id '{dataset_id}'")

    c = matches.group(2)

    target = client.datasets.clean if int(c) > 0 else client.datasets.raw

    if version in ('', 'latest'):
        insp = target.inspect(dataset_id)
        insp.raise_for_status()
        try:
            resolved = _version_id_from_inspect_json(insp.json())
        except (ValueError, KeyError, IndexError):
            resolved = 'latest'
        Datasets.DEPENDENCIES[dataset_id] = resolved
        read = target.read(dataset_id)
    else:
        Datasets.DEPENDENCIES[dataset_id] = version
        insp = target.inspect(dataset_id, version=version)
        insp.raise_for_status()
        read = target.read(dataset_id, version=version)

    read.raise_for_status()
    return read.content


def _default_download(dataset_id: str, version: str, to: str | Path):
    data = _default_get(dataset_id, version)
    Path(to).write_bytes(data)


class Proxy:
    def __init__(self, dataset_id: str, parent: 'Datasets') -> None:
        self.dataset_id = dataset_id
        self.parent = parent

    def get(self, by: None|DatasetGetter=None, version: None|str=None):
        version = version or 'latest'
        getter = by or _default_get
        return getter(self.dataset_id, version)
    
    def download(self, to: str|Path, by: None|DatasetDownloader=None, version: None|str=None):
        version = version or 'latest'
        downloader = by or _default_download
        return downloader(self.dataset_id, version, to)

    def register(self, filepath: str, **kwargs):
        data = dict(filepath=filepath, **kwargs)
        self.parent.REGISTRY[self.dataset_id] = data
        return self
    
    def save(self, /, obj, func=None, **kwargs):
        self.parent.EXPORTS.add(self.dataset_id)

        if func is None:
            return obj 

        return func(obj, **kwargs)

class Datasets(type):
    """Maps dependency dataset id -> version token (commit hash, pin, or literal ``latest``)."""
    DEPENDENCIES: dict[str, str] = {}
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
        """Each entry is ``dataset_id@version`` (hash, pin, or ``latest``)."""
        dependencies: list[str]
        registry: dict[str, dict[str, object]]
        exports: list[str]

    @classmethod
    def get_representation(cls):
        return cls._Representation(
            dependencies=[
                f'{dataset_id}@{ver}'
                for dataset_id, ver in cls.DEPENDENCIES.items()
            ],
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
