"""Contract test: any ingestor must pass these. Add new domains to PARAMS-style fixtures."""
import pandas as pd

from ingestion.base import BaseIngestor
from ingestion.config import DomainConfig
from ingestion.runner import run_all
from ingestion.stores import FileWatermarkStore, ParquetSink


class FakeIngestor(BaseIngestor):
    def extract(self, ctx):
        df = pd.DataFrame({"id": [1, 2, 3], "updated_at": ["2026-10-01", "2026-10-02", "2026-10-03"]})
        if ctx.watermark_in:
            df = df[df["updated_at"] > ctx.watermark_in]
        return df


def make(tmp_path, cls=FakeIngestor, name="fake"):
    cfg = DomainConfig(name=name, source_dsn_env="X", source_table="t", source_schema_ref="a",
                       target_schema_ref="b", primary_keys=["id"], watermark_column="updated_at")
    return cls(cfg, watermarks=FileWatermarkStore(tmp_path / "wm.json"), sink=ParquetSink(tmp_path / "data"))


def test_success_and_incremental(tmp_path):
    ing = make(tmp_path)
    r1 = ing.run()
    assert r1.status == "success" and r1.counts["loaded"] == 3
    assert r1.watermark_out == "2026-10-03"
    r2 = ing.run()                      # nothing new after the watermark
    assert r2.status == "success" and r2.counts["extracted"] == 0


def test_failure_is_contained_and_watermark_not_advanced(tmp_path):
    class Boom(FakeIngestor):
        def extract(self, ctx):
            raise RuntimeError("source down")
    ing = make(tmp_path, Boom)
    r = ing.run()
    assert r.status == "failed" and "source down" in r.error
    assert ing.watermarks.get("fake") is None


def test_validation_catches_missing_key_column(tmp_path):
    class NoKey(FakeIngestor):
        def extract(self, ctx):
            return pd.DataFrame({"updated_at": ["2026-10-01"]})
    r = make(tmp_path, NoKey).run()
    assert r.status == "failed" and "missing required columns" in r.error
