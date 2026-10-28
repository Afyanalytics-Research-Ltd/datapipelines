"""Reference domain: a thin subclass adding one domain-specific rule on top of the
generic SQL extract. Copy this file as the template for a new domain that needs custom logic."""
from __future__ import annotations

import pandas as pd

from ..base import ValidationError
from ..context import RunContext
from ..registry import register
from .generic_sql import GenericSqlIngestor


@register("patients")
class PatientsIngestor(GenericSqlIngestor):
    def validate(self, df: pd.DataFrame, ctx: RunContext) -> None:
        super().validate(df, ctx)
        if "date_of_birth" in df.columns and (pd.to_datetime(df["date_of_birth"], errors="coerce") > pd.Timestamp.now()).any():
            raise ValidationError("patients: date_of_birth in the future")
