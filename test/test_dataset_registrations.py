import rootdir
import pytest
import ast

from argendata_datasets.dsl.analyzer import get_dataset_registrations
from argendata_datasets.dsl.analyzer import Name

PROGRAM = """
from argendata.datasets import Datasets
import pathlib

x = Datasets.R1C0.get(version='latest')

dataset = Datasets.R1C1.register(
    filename=FILENAME,
)

dataset.save(...)
"""

@pytest.fixture
def program_ast():
    return ast.parse(PROGRAM)

def test_get_dataset_registrations(program_ast):
    registrations = get_dataset_registrations(program_ast)
    assert len(registrations) == 1
    assert registrations[0]['symbol'] == 'dataset'
    assert registrations[0]['registration'].name == 'R1C1'
    assert isinstance(registrations[0]['registration'].filename, Name)
    assert registrations[0]['registration'].filename.id == 'FILENAME'
    assert registrations[0]['registration'].stmt_index == 3
    