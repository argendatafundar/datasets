from dataclasses import dataclass
import ast

@dataclass(frozen=True)
class Request:
    name: str
    method: str
    version: str | None = None

    def __str__(self):
        return f"{self.name}@{self.version}"

def parse_node(node: ast.expr) -> None|Request:
    if not isinstance(node, ast.Call):
        return None
    # Expect: Datasets.NAME.method(...)
    if not isinstance(node.func, ast.Attribute):
        return None

    # node.func.value should be Datasets.NAME
    dataset_attr = node.func.value
    if not isinstance(dataset_attr, ast.Attribute):
        return None

    # dataset_attr.value should be the Name 'Datasets'
    if not isinstance(dataset_attr.value, ast.Name):
        return None
    if dataset_attr.value.id != 'Datasets':
        return None

    dataset_name = dataset_attr.attr
    method_name = node.func.attr

    if method_name == 'register':
        return None

    version = None
    for kw in node.keywords:
        if kw.arg == 'version' and isinstance(kw.value, ast.Constant):
            version = kw.value.value
            break
    
    return Request(name=dataset_name, version=version, method=method_name)


def get_datasets(text: str) -> set[Request]:
    """
    Parse AST to find all Datasets.X.method(version=...) calls.
    Returns a set of Dataset(name, version) objects.
    """
    tree = ast.parse(text.strip())
    datasets = {
        request 
        for request in map(parse_node, ast.walk(tree)) 
        if request is not None
    }

    return datasets