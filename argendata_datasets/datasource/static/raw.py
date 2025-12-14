import urllib.request
import urllib.parse
import pathlib
import io
from .utils import make_request

def get_index(
    hostname: str,
    scheme='http',
    path='/static/etl-fuentes/fuentes_raw.csv',
    params='',
    query='',
    fragment='',
):
    import polars as pl
    request = make_request(
        hostname=hostname,
        scheme=scheme,
        path=path,
        params=params,
        query=query,
        fragment=fragment,
    )

    response = urllib.request.urlopen(request)
    return pl.read_csv(response)

def get_by_filename(
    filename: str,
    hostname: str,
    scheme='http',
    path='/static/etl-fuentes/raw',
    params='',
    query='',
    fragment='',
) -> io.BytesIO:

    path = pathlib.Path(path) / filename
    path = str(path)

    request = make_request(
        scheme=scheme,
        netloc=hostname,
        path=path,
        params=params,
        query=query,
        fragment=fragment,
    )

    response = urllib.request.urlopen(request)
    return io.BytesIO(response.read())

def download_by_filename(
    filename: str,
    hostname: str,
    to: str|pathlib.Path,
    chunk_size: int = 16784000, # 16MB
    scheme='http',
    path='/static/etl-fuentes/raw',
    params='',
    query='',
    fragment='',
) -> pathlib.Path:
    to = pathlib.Path(to)

    path = pathlib.Path(path) / filename
    path = str(path)

    request = make_request(
        scheme=scheme,
        netloc=hostname,
        path=path,
        params=params,
        query=query,
        fragment=fragment,
    )

    response = urllib.request.urlopen(request)
    
    with to.open('wb') as f:
        while True:
            chunk = response.read(chunk_size)
            if not chunk:
                break
            f.write(chunk)

    return to