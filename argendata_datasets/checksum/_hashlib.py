def get_hash_method(name: str):
    import hashlib
    if name not in hashlib.algorithms_available:
        raise AttributeError(
            f"Algorithm '{name}' is not available, "
            + f"must be one of "
            + ', '.join(f'{x}' for x in hashlib.algorithms_available))
    return getattr(hashlib, name)