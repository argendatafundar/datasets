import rootdir
from argendata_datasets.dsl.analyzer import get_datasets, parse_node, Request
from argendata_datasets.dsl.datasets import Datasets
import ast, pathlib

def test_get_datasets_valid():
    testcase = "Datasets.R1C0.method(version='latest')"

    assert get_datasets(testcase) == {Request(name='R1C0', method='method', version='latest')}

    testcase =\
        """
        (Datasets
            .R1C0
            .method(
            version='latest')
        )
        """

    assert get_datasets(testcase) == {Request(name='R1C0', method='method', version='latest')}

def dataset_getter_mock(uri: str) -> str:
    return uri

def dataset_downloader_mock(uri: str, to: pathlib.Path) -> pathlib.Path:
    return to / uri

def test_client():
    assert Datasets.used == set()
    
    assert (
        Datasets.R1C0.get(
            version='latest', 
            by=dataset_getter_mock) == 'R1C0@latest'
    )

    assert Datasets.used == {'R1C0@latest'}
    
    assert (
        Datasets.R1C0.download(
            to='.', 
            version='latest', 
            by=dataset_downloader_mock) == pathlib.Path('R1C0@latest')
    )

    assert Datasets.used == {'R1C0@latest'}

    assert (
        Datasets.R1C2.get(
            version='latest', 
            by=dataset_getter_mock) == 'R1C2@latest'
    )

    assert Datasets.used == {'R1C0@latest', 'R1C2@latest'}

    assert (
        Datasets.R1C2.download(
            to='.', 
            version='latest', 
            by=dataset_downloader_mock) == pathlib.Path('R1C2@latest')
    )

    assert Datasets.used == {'R1C0@latest', 'R1C2@latest'}

    # singleton behavior
    from argendata_datasets.dsl.datasets import Client
    client = Client()

    assert client is Datasets
