import rootdir
from argendata_datasets.checksum import hash, digest, Hash

def test_hash():
    result = hash.sha1(b'hello')
    expected_hexdigest = 'sha1:aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d'

    assert isinstance(result, Hash)
    assert result.to_str() == expected_hexdigest

def test_digest():
    import tempfile, pathlib

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = pathlib.Path(tmp_dir) / 'test.txt'
        tmp_path.write_text('hello')
        result = digest.sha1(tmp_path)
        assert isinstance(result, Hash)
        assert result.to_str() == f'{str(tmp_path)}@sha1:aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d'
    
    import io

    with io.BytesIO(b'hello') as f:
        result = digest.sha1(f)
        assert isinstance(result, Hash)
        assert result.to_str() == 'sha1:aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d'
    
def test_roundtrip_hash():
    hashobj = Hash(method='sha1', hexdigest='aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d')
    assert hashobj.to_str() == str(hashobj)
    assert Hash.from_str(str(hashobj)) == hashobj

    hashobj = Hash(method='sha1', hexdigest='aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d', filename='test.txt')
    assert hashobj.to_str() == str(hashobj)
    assert Hash.from_str(str(hashobj)) == hashobj