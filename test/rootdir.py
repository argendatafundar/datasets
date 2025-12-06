import os, sys

ROOT_DIR = next(( os.path.join(root, "pyproject.toml")
                for root, _, files in os.walk(os.getcwd())
                if "pyproject.toml" in files
                ), None)

sys.path.append(os.path.dirname(ROOT_DIR))