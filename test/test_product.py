from argendata_datasets import checksum
from argendata_datasets.utils import Product

def test_product():
    codigo = 'R1C0'
    method = 'sha1'
    filename = 'output.csv'
    hexdigest = 'aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d'
    hash = checksum.Hash.from_str(f'{filename}@{method}:{hexdigest}')
    
    product = Product.from_str(f'{codigo}({hash})')
    assert product.codigo == codigo
    assert product.checksum == hash

    product = Product.from_str(f'{filename}@{method}:{hexdigest}')
    assert product.codigo is None
    assert product.checksum == hash

    product = Product.from_str(f'{method}:{hexdigest}')
    assert product.codigo is None
    assert product.checksum == checksum.Hash(method=method, hexdigest=hexdigest)