import ast
import re
from dataclasses import dataclass
from typing import TypedDict

@dataclass(frozen=True)
class Constant:
    value: str

@dataclass(frozen=True)
class Name:
    id: str

@dataclass(frozen=True)
class DatasetRegister:
    symbol: str
    name: str
    filename: str
    stmt_index: int

@dataclass(frozen=True)
class DatasetSave:
    symbol: str
    node: ast.expr

class Symbol(TypedDict):
    """
    Representa el simbolo utilizado en el script para registrar y guardar el dataset.
    Esto es así para forzar el contexto register-save.

    Ejemplos:

    ### OK:
    >>> ds = Datasets.R1C0.register(filename='data.csv')
    >>> # ...
    >>> ds.save(...)

    En este caso, el simbolo es 'ds' y posee su registro en la linea 1,
    y su guardado al final del script.

    ### Missing save
    >>> ds = Datasets.R1C0.register(filename='data.csv')
    >>> # ...

    ### Missing register
    >>> # ...
    >>> Datasets.R1C0.save(...)
    """
    symbol: str
    registration: DatasetRegister
    save: DatasetSave

def get_dataset_registrations(tree: ast.Module) -> list[Symbol]:
    statements = tree.body
    indexed_assignments = [
        (statement, i) 
        for i, statement in enumerate(statements) 
        if isinstance(statement, ast.Assign)
    ]

    if len(indexed_assignments) == 0:
        raise ValueError("No assignment found")


    def parse_register(assignment: ast.Assign, stmt_index: int) -> None|DatasetRegister:
        symbol = assignment.targets[0].id
        
        name = assignment.value.func.value.attr
        filename = next((x.value
            for x in assignment.value.keywords
            if isinstance(x, ast.keyword)
            if x.arg == 'filename'
        ), None)

        if not (isinstance(filename, ast.Constant) or isinstance(filename, ast.Name)):
            return None
        
        if isinstance(filename, ast.Constant):
            filename = Constant(value=filename.value)
        elif isinstance(filename, ast.Name):
            filename = Name(id=filename.id)
        
        return DatasetRegister(symbol=symbol, name=name, filename=filename, stmt_index=stmt_index)

    registrations = [
        parse_register(assignment, i)
        for assignment, i in indexed_assignments
        if isinstance(assignment.value, ast.Call)
        if isinstance(assignment.value.func, ast.Attribute)
        if isinstance(assignment.value.func.value, ast.Attribute)
        if assignment.value.func.value.value.id == 'Datasets'
        if re.match(r"^R\d+C\d+$", assignment.value.func.value.attr)
        if assignment.value.func.attr == 'register'
    ]

    if len(registrations) == 0:
        raise ValueError("No registrations found")

    calls = [
        statement
        for statement in ast.walk(tree)
        if isinstance(statement, ast.Call)
    ]

    dataset_saves = [
        x
        for x in calls
        if isinstance(x.func, ast.Attribute)
        if isinstance(x.func.value, ast.Name)
        if any(x.func.value.id == registration.symbol for registration in registrations)
    ]

    if len(dataset_saves) == 0:
        raise ValueError("No dataset saves found")

    if len(dataset_saves) != len(registrations):
        raise ValueError("Number of dataset saves does not match number of registrations")

    result = [
            {
            "symbol": symbol,
            "save": DatasetSave(
                symbol=symbol, 
                node=save
            ),

            "registration": next((
                registration
                for registration in registrations
                if registration.symbol == symbol
            ), None)
            }

        for save in dataset_saves
        for symbol in (save.func.value.id, )
    ]

    return result