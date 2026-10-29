from __future__ import annotations

import importlib
import pkgutil

_REGISTRY: dict[str, type] = {}


def register(key: str):
    def deco(cls):
        _REGISTRY[key] = cls
        return cls
    return deco


def get_ingestor_cls(key: str) -> type:
    if key not in _REGISTRY:
        raise KeyError(f"No ingestor registered as '{key}'. Known: {sorted(_REGISTRY)}")
    return _REGISTRY[key]


def discover_domains() -> None:
    """Import every module in ingestion.domains so its @register runs."""
    from . import domains
    for m in pkgutil.iter_modules(domains.__path__):
        importlib.import_module(f"{domains.__name__}.{m.name}")
