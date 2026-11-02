from __future__ import annotations

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
