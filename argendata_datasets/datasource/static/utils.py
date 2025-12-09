import urllib.request
import urllib.parse

def make_request(
    hostname: str,
    scheme: str,
    path: str,
    params: str,
    query: str,
    fragment: str,
    method: None|str = None,
) -> urllib.request.Request:

    parts = urllib.parse.ParseResult(
        scheme=scheme,
        netloc=hostname,
        path=path,
        params=params,
        query=query,
        fragment=fragment,
    )

    url = parts.geturl()
    return urllib.request.Request(url, method=method)