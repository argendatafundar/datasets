import urllib.request
import urllib.parse
import io
import pathlib

def get_index(
    hostname: str,
    scheme='http',
    path='/static/etl-fuentes/fuentes_clean.csv',
    params='',
    query='',
    fragment='',
):
    import polars as pl
    parts = urllib.parse.ParseResult(
        scheme=scheme,
        netloc=hostname,
        path=path,
        params=params,
        query=query,
        fragment=fragment,
    )

    url = parts.geturl()
    request = urllib.request.Request(url)
    response = urllib.request.urlopen(request)
    return pl.read_csv(response)

def get_by_filename(
    filename: str,
    hostname: str,
    scheme='http',
    path='/static/etl-fuentes/clean',
    params='',
    query='',
    fragment='',
):
    import polars as pl
    path = pathlib.Path(path) / filename
    path = str(path)

    parts = urllib.parse.ParseResult(
        scheme=scheme,
        netloc=hostname,
        path=path,
        params=params,
        query=query,
        fragment=fragment,
    )

    url = parts.geturl()
    request = urllib.request.Request(url)
    response = urllib.request.urlopen(request)
    return pl.read_parquet(response)