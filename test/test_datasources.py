import rootdir

from dataclasses import dataclass
import argendata_datasets
import dotenv
import pytest

@dataclass
class Fixture:
    static_hostname: str

@pytest.fixture
def fixture():
    return Fixture(
        static_hostname = dotenv.dotenv_values()['STATIC_HOSTNAME'],
    )

def test_static(fixture):
    import polars as pl
    static_hostname = fixture.static_hostname

    raw_index = argendata_datasets.static.raw.get_index(static_hostname)
    assert isinstance(raw_index, pl.DataFrame)

    clean_index = argendata_datasets.static.clean.get_index(static_hostname)
    assert isinstance(clean_index, pl.DataFrame)

    clean_by_filename = argendata_datasets.static.clean.get_by_filename(
        filename='weo_imf.parquet',
        hostname=static_hostname,
    )

    assert isinstance(clean_by_filename, pl.DataFrame)