from __future__ import annotations

import json
from typing import Any, Iterable, Mapping

__all__ = ["normalize_names", "resolve_selection", "selection_params"]


def normalize_names(value: Iterable[str] | str | None) -> list[str]:
    """
    Coerce a DAG param into a clean list of names.

    Handles the shapes Airflow can hand you:
      None / "" / []          -> []
      ["a", "b"]              -> ["a", "b"]
      '["a", "b"]'            -> ["a", "b"]   (JSON string, native objects off)
      "a, b"                  -> ["a", "b"]   (comma-separated string)
    """
    if value is None:
        return []

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if text.startswith("["):
            try:
                value = json.loads(text)
            except json.JSONDecodeError:
                value = text.strip("[]").split(",")
        else:
            value = text.split(",")

    return [str(v).strip().strip("'\"") for v in value if str(v).strip()]


def resolve_selection(
    items: Mapping[str, Mapping[str, Any]],
    *,
    only: Iterable[str] | str | None = None,
    exclude: Iterable[str] | str | None = None,
    kinds: Iterable[str] | None = None,
    kind_key: str = "kind",
    strict: bool = True,
) -> list[str]:
    """
    Decide which tables/models a pipeline run should process.

    items    : {name: spec} for ONE pipeline. spec is a mapping that
               contains `kind_key` when `kinds` is used.
    only     : if non-empty, restrict to these names.
    exclude  : remove these names (applied after `only`).
    kinds    : eligible kinds for this pipeline. None = every item eligible.
    kind_key : key in each spec that holds its kind.
    strict   : raise on names in only/exclude that are not in `items`,
               so a typo in a DAG param can't silently skip a table.

    Returns names in the original order of `items`, so any dependency
    ordering defined in the registry (parents before children) is kept.
    """
    only_s = set(normalize_names(only))
    exclude_s = set(normalize_names(exclude))

    if strict:
        unknown = (only_s | exclude_s) - set(items)
        if unknown:
            raise ValueError(
                f"Unknown names: {sorted(unknown)}. Known: {sorted(items)}"
            )

    kinds_s = set(kinds) if kinds is not None else None

    selected = {
        name
        for name, spec in items.items()
        if kinds_s is None or spec.get(kind_key) in kinds_s
    }

    if only_s:
        selected &= only_s
    selected -= exclude_s

    return [name for name in items if name in selected]


def selection_params(names: Iterable[str]) -> dict:
    """
    Build Airflow `params` giving a multi-select `only` / `exclude` in the
    trigger UI, populated from one pipeline's own names.

        with DAG(..., params=selection_params(sorted(MODELS)),
                 render_template_as_native_obj=True): ...
    """
    from airflow.models.param import Param  # lazy: keeps this module importable without Airflow

    enum = sorted(names)
    return {
        "only": Param(
            [],
            type="array",
            items={"type": "string", "enum": enum},
            description="Only process these. Leave empty for all.",
        ),
        "exclude": Param(
            [],
            type="array",
            items={"type": "string", "enum": enum},
            description="Skip these.",
        ),
    }