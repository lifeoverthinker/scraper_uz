from __future__ import annotations

import importlib
import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Optional

__all__ = [
    "sprawdz_nieregularne_zajecia",
    "parse_nauczyciele_from_group_page",
    "parse_nauczyciel_details",
]

# avoid importing this wrapper module name to prevent circular import
CANDIDATE_MODULE_NAMES = [
    "scraper.nauczyciel_parser",
    "nauczyciel_parser",
]

def _try_import(name: str) -> Optional[ModuleType]:
    try:
        return importlib.import_module(name)
    except Exception:
        return None

def _try_load_file(path: Path) -> Optional[ModuleType]:
    try:
        spec = importlib.util.spec_from_file_location("dynamic_nauczyciel_parser", str(path))
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)  # type: ignore
            return module
    except Exception:
        return None
    return None

_module: Optional[ModuleType] = None
for nm in CANDIDATE_MODULE_NAMES:
    mod = _try_import(nm)
    if mod is not None:
        _module = mod
        break

if _module is None:
    here = Path(__file__).resolve().parent
    candidates = [
        here.parent / "nauczyciel_parser.py",
        here.parent / "nauczyciel_parser_impl.py",
        here / "nauczyciel_parser.py",
        Path.cwd() / "scraper" / "nauczyciel_parser.py",
        Path.cwd() / "nauczyciel_parser.py",
    ]
    for p in candidates:
        if p.exists() and p.resolve() != Path(__file__).resolve():
            mod = _try_load_file(p)
            if mod is not None:
                _module = mod
                break

if _module is None:
    checked = ", ".join(repr(str(p)) for p in candidates)
    raise ImportError(
        "scraper.parsers.nauczyciel_parser: could not locate implementation. "
        f"Checked module names: {CANDIDATE_MODULE_NAMES} and files: {checked}."
    )

try:
    sprawdz_nieregularne_zajecia = getattr(_module, "sprawdz_nieregularne_zajecia")
    parse_nauczyciele_from_group_page = getattr(_module, "parse_nauczyciele_from_group_page")
    parse_nauczyciel_details = getattr(_module, "parse_nauczyciel_details")
except AttributeError as e:
    raise ImportError(f"scraper.parsers.nauczyciel_parser: expected attributes not found: {e}") from e