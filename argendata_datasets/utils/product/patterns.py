CODIGO = r'[a-zA-Z0-9_]+'
HASH = r'(?:(.*)@)?([a-zA-Z0-9_]+):([a-fA-F0-9]+)' # (filename@)?method:hexdigest
HASH_NON_CAPTURING = r'(?:.*@)?[a-zA-Z0-9_]+:[a-fA-F0-9]+' # non-capturing version for use in PRODUCT
PRODUCT = rf'({CODIGO})\(({HASH_NON_CAPTURING})\)'