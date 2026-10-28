from __future__ import annotations

import importlib
import pkgutil

_REGISTRY: dict[str, type] = {}


def register(key: str):
    """Class decorator: @register("v3_to_v3") makes the class discoverable by key."""

    def deco(cls):
        existing = _REGISTRY.get(key)
        if existing is not None and (
            existing.__module__,
            existing.__qualname__,
        ) != (cls.__module__, cls.__qualname__):
            raise ValueError(
                f"Ingestor key '{key}' already registered by "
                f"{existing.__module__}.{existing.__qualname__}"
            )
        _REGISTRY[key] = cls
        return cls

    return deco


def get_ingestor_cls(key: str) -> type:
    if key not in _REGISTRY:
        raise KeyError(
            f"No ingestor registered as '{key}'. Known: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[key]


def list_registered() -> dict[str, type]:
    """Read-only snapshot, e.g. for a DAG factory looping over every pipeline."""
    return dict(_REGISTRY)


def discover_domains() -> None:
    """Import every module in ingestion.domains so its @register runs."""
    from . import domains

    for m in pkgutil.iter_modules(domains.__path__):
        importlib.import_module(f"{domains.__name__}.{m.name}")