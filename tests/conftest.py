import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "shared"))


def load(rel: str):
    """A validator script as a module (they share the name typecheck)."""
    spec = importlib.util.spec_from_file_location(rel.replace("/", "_").removesuffix(".py"), ROOT / rel)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
