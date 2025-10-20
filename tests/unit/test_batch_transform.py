"""
Unit tests for batch_transform.normalize_columns and enrich_with_hash using pytest.
"""
import pandas as pd
from processing.batch_transform import normalize_columns, enrich_with_hash


def test_normalize_columns():
    df = pd.DataFrame({"A Col": [1], " Another": [2]})
    out = normalize_columns(df)
    assert "a_col" in out.columns
    assert "another" in out.columns


def test_enrich_with_hash():
    df = pd.DataFrame({"id": [1], "value": ["x"]})
    out = enrich_with_hash(df, ["id", "value"])
    assert "record_hash" in out.columns
    assert out["record_hash"].iloc[0] is not None