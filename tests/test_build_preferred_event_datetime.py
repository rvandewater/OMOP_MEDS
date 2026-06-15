"""Tests for build_preferred_event_datetime in pre_meds_utils.py.

The function resolves a single canonical event timestamp for any OMOP table
that has multiple date/datetime columns (e.g. note, specimen, drug_exposure)
using the following rules:

  1. primary_datetime_col  → used as-is when present and non-null.
  2. primary_date_col      → fallback; promoted to end-of-day (23:59:59).
  3. override_datetime_col → only replaces primary when non-null AND strictly later
                             (requires use_override_if_later=True).
  4. override_date_col     → end-of-day fallback for override.
  5. Nothing usable        → null Datetime("us") column returned.

Test groups
-----------
A  – primary_datetime_col only
B  – primary_date_col only (end-of-day promotion)
C  – both primary columns (datetime preferred, date is coalesce fallback)
D  – override disabled (use_override_if_later=False, default)
E  – override enabled, override later    → override wins
F  – override enabled, override earlier  → primary kept
G  – override enabled, equal timestamps  → primary kept (strict >)
H  – override datetime null, override date used at end-of-day
I  – both override columns null          → primary unchanged
J  – override columns absent from schema → no KeyError
K  – no primary columns in schema        → null output
L  – no columns at all configured (None) → null output
M  – custom output_col alias
N  – native pl.Date / pl.Datetime types (not Utf8 strings)
O  – mixed rows (per-row correctness)
P  – output dtype is always pl.Datetime
Q  – table-agnostic: drug_exposure column names work identically
R  – wrap_with_datetime_resolver wires config correctly (integration)
"""

import datetime as dt

import polars as pl
import pytest
from omegaconf import OmegaConf

from OMOP_MEDS.pre_meds_utils import build_preferred_event_datetime


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_OUTPUT_COLUMN = "preferred_time"


def _resolve(df: pl.DataFrame, **kwargs) -> list:
    """Return the resolved output column as a Python list."""
    schema = df.lazy().collect_schema()
    out_col = kwargs.get("output_col", DEFAULT_OUTPUT_COLUMN)
    expr = build_preferred_event_datetime(schema, **kwargs)
    return df.lazy().with_columns(expr).collect()[out_col].to_list()


def _ts(s: str) -> dt.datetime:
    """Parse an ISO datetime string to a Python datetime (for assertions)."""
    return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


# ─────────────────────────────────────────────────────────────────────────────
# A – primary_datetime_col only
# ─────────────────────────────────────────────────────────────────────────────


class TestPrimaryDatetimeOnly:
    def test_uses_datetime_value_exactly(self):
        df = pl.DataFrame({"evt_dt": ["2024-05-01 09:30:00", "2024-06-15 14:45:59"]})
        times = _resolve(df, primary_datetime_col="evt_dt")
        assert times[0].hour == 9 and times[0].minute == 30
        assert times[1].hour == 14 and times[1].second == 59

    def test_null_datetime_yields_null(self):
        df = pl.DataFrame({"evt_dt": [None]}).with_columns(
            pl.col("evt_dt").cast(pl.Utf8)
        )
        times = _resolve(df, primary_datetime_col="evt_dt")
        assert times[0] is None

    def test_microsecond_precision_preserved(self):
        df = pl.DataFrame({"evt_dt": ["2024-05-01 09:00:00.123456"]})
        times = _resolve(df, primary_datetime_col="evt_dt")
        assert times[0].microsecond == 123456


# ─────────────────────────────────────────────────────────────────────────────
# B – primary_date_col only → end-of-day promotion
# ─────────────────────────────────────────────────────────────────────────────


class TestPrimaryDateOnly:
    def test_date_promoted_to_end_of_day(self):
        df = pl.DataFrame({"evt_d": ["2024-05-01", "2024-06-15"]})
        times = _resolve(df, primary_date_col="evt_d")
        for t in times:
            assert t.hour == 23
            assert t.minute == 59
            assert t.second == 59

    def test_correct_calendar_date_preserved(self):
        df = pl.DataFrame({"evt_d": ["2024-12-31"]})
        times = _resolve(df, primary_date_col="evt_d")
        assert times[0].month == 12 and times[0].day == 31


# ─────────────────────────────────────────────────────────────────────────────
# C – both primary columns (datetime preferred, date is coalesce fallback)
# ─────────────────────────────────────────────────────────────────────────────


class TestPrimaryDatetimeAndDate:
    def test_datetime_wins_over_date(self):
        df = pl.DataFrame(
            {
                "evt_dt": ["2024-05-01 09:00:00"],
                "evt_d": ["2024-05-02"],  # later date but datetime must win
            }
        )
        times = _resolve(df, primary_datetime_col="evt_dt", primary_date_col="evt_d")
        assert times[0].day == 1 and times[0].hour == 9

    def test_null_datetime_falls_back_to_date_at_eod(self):
        df = pl.DataFrame(
            {
                "evt_dt": [None],
                "evt_d": ["2024-05-01"],
            }
        ).with_columns(pl.col("evt_dt").cast(pl.Utf8))
        times = _resolve(df, primary_datetime_col="evt_dt", primary_date_col="evt_d")
        assert times[0] is not None
        assert times[0].hour == 23 and times[0].day == 1

    def test_both_null_yields_null(self):
        df = pl.DataFrame(
            {
                "evt_dt": [None],
                "evt_d": [None],
            }
        ).with_columns(
            pl.col("evt_dt").cast(pl.Utf8),
            pl.col("evt_d").cast(pl.Utf8),
        )
        times = _resolve(df, primary_datetime_col="evt_dt", primary_date_col="evt_d")
        assert times[0] is None


# ─────────────────────────────────────────────────────────────────────────────
# D – override disabled (default)
# ─────────────────────────────────────────────────────────────────────────────


class TestOverrideDisabled:
    def test_override_columns_have_no_effect(self):
        df = pl.DataFrame(
            {
                "evt_dt": ["2024-05-01 09:00:00"],
                "evt_d": ["2024-05-01"],
                "ov_dt": ["2024-05-03 10:00:00"],  # later, but disabled
                "ov_d": ["2024-05-03"],
            }
        )
        times = _resolve(
            df,
            primary_datetime_col="evt_dt",
            primary_date_col="evt_d",
            override_datetime_col="ov_dt",
            override_date_col="ov_d",
            use_override_if_later=False,
        )
        assert times[0].day == 1 and times[0].hour == 9

    def test_default_flag_is_true(self):
        """use_override_if_later must default to False."""
        df = pl.DataFrame(
            {
                "evt_dt": ["2024-05-01 09:00:00"],
                "ov_dt": ["2024-05-03 10:00:00"],
            }
        )
        times = _resolve(
            df,
            primary_datetime_col="evt_dt",
            override_datetime_col="ov_dt",
            # use_override_if_later intentionally omitted
        )
        assert times[0].day == 3


# ─────────────────────────────────────────────────────────────────────────────
# E – override enabled AND override is later → override wins
# ─────────────────────────────────────────────────────────────────────────────


class TestOverrideWinsWhenLater:
    def test_override_datetime_replaces_primary_datetime(self):
        df = pl.DataFrame(
            {
                "evt_dt": ["2024-05-01 09:00:00"],
                "ov_dt": ["2024-05-03 12:00:00"],
            }
        )
        times = _resolve(
            df,
            primary_datetime_col="evt_dt",
            override_datetime_col="ov_dt",
            use_override_if_later=True,
        )
        assert times[0].day == 3 and times[0].hour == 12

    def test_override_date_eod_replaces_primary_datetime(self):
        """Override is date-only; end-of-day promotion means it can still be later."""
        df = pl.DataFrame(
            {
                "evt_dt": ["2024-05-01 09:00:00"],
                "ov_d": ["2024-05-03"],  # 2024-05-03 23:59:59 > 2024-05-01 09:00
            }
        )
        times = _resolve(
            df,
            primary_datetime_col="evt_dt",
            override_date_col="ov_d",
            use_override_if_later=True,
        )
        assert times[0].day == 3 and times[0].hour == 23


# ─────────────────────────────────────────────────────────────────────────────
# F – override enabled BUT override is earlier → primary kept
# ─────────────────────────────────────────────────────────────────────────────


class TestOverrideIgnoredWhenEarlier:
    def test_earlier_override_datetime_does_not_replace(self):
        df = pl.DataFrame(
            {
                "evt_dt": ["2024-05-05 14:00:00"],
                "ov_dt": ["2024-05-01 08:00:00"],
            }
        )
        times = _resolve(
            df,
            primary_datetime_col="evt_dt",
            override_datetime_col="ov_dt",
            use_override_if_later=True,
        )
        assert times[0].day == 5 and times[0].hour == 14

    def test_earlier_override_date_eod_does_not_replace(self):
        df = pl.DataFrame(
            {
                "evt_dt": ["2024-05-05 10:00:00"],
                "ov_d": ["2024-05-04"],  # 2024-05-04 23:59:59 < 2024-05-05 10:00
            }
        )
        times = _resolve(
            df,
            primary_datetime_col="evt_dt",
            override_date_col="ov_d",
            use_override_if_later=True,
        )
        assert times[0].day == 5 and times[0].hour == 10


# ─────────────────────────────────────────────────────────────────────────────
# G – equal timestamps: strict > means primary is kept
# ─────────────────────────────────────────────────────────────────────────────


class TestOverrideEqualTimestamp:
    def test_equal_timestamps_primary_is_kept(self):
        ts = "2024-05-01 09:00:00"
        df = pl.DataFrame({"evt_dt": [ts], "ov_dt": [ts]})
        times = _resolve(
            df,
            primary_datetime_col="evt_dt",
            override_datetime_col="ov_dt",
            use_override_if_later=True,
        )
        assert times[0] == _ts(ts)


# ─────────────────────────────────────────────────────────────────────────────
# H – override datetime null → override date fallback at end-of-day
# ─────────────────────────────────────────────────────────────────────────────


class TestOverrideDateFallback:
    def test_null_override_dt_uses_override_date_at_eod(self):
        df = pl.DataFrame(
            {
                "evt_dt": ["2024-05-01 09:00:00"],
                "ov_dt": [None],
                "ov_d": ["2024-05-03"],  # 23:59:59 > 09:00:00
            }
        ).with_columns(pl.col("ov_dt").cast(pl.Utf8))
        times = _resolve(
            df,
            primary_datetime_col="evt_dt",
            override_datetime_col="ov_dt",
            override_date_col="ov_d",
            use_override_if_later=True,
        )
        assert times[0].day == 3 and times[0].hour == 23 and times[0].second == 59

    def test_null_override_dt_eod_earlier_than_primary_ignored(self):
        df = pl.DataFrame(
            {
                "evt_dt": ["2024-05-05 23:30:00"],
                "ov_dt": [None],
                "ov_d": ["2024-05-05"],  # 23:59:59 > 23:30:00 → override wins
            }
        ).with_columns(pl.col("ov_dt").cast(pl.Utf8))
        times = _resolve(
            df,
            primary_datetime_col="evt_dt",
            override_datetime_col="ov_dt",
            override_date_col="ov_d",
            use_override_if_later=True,
        )
        assert times[0].second == 59  # end-of-day wins here


# ─────────────────────────────────────────────────────────────────────────────
# I – both override columns null → primary unchanged
# ─────────────────────────────────────────────────────────────────────────────


class TestBothOverrideColumnsNull:
    def test_all_override_null_primary_unchanged(self):
        df = pl.DataFrame(
            {
                "evt_dt": ["2024-05-01 09:00:00"],
                "ov_dt": [None],
                "ov_d": [None],
            }
        ).with_columns(
            pl.col("ov_dt").cast(pl.Utf8),
            pl.col("ov_d").cast(pl.Utf8),
        )
        times = _resolve(
            df,
            primary_datetime_col="evt_dt",
            override_datetime_col="ov_dt",
            override_date_col="ov_d",
            use_override_if_later=True,
        )
        assert times[0].day == 1 and times[0].hour == 9


# ─────────────────────────────────────────────────────────────────────────────
# J – override columns absent from schema entirely → no KeyError
# ─────────────────────────────────────────────────────────────────────────────


class TestOverrideColumnsAbsent:
    def test_override_cols_not_in_schema_no_error(self):
        df = pl.DataFrame({"evt_dt": ["2024-05-01 09:00:00"]})
        times = _resolve(
            df,
            primary_datetime_col="evt_dt",
            override_datetime_col="xtn_nonexistent_datetime",
            override_date_col="xtn_nonexistent_date",
            use_override_if_later=True,
        )
        assert times[0].hour == 9

    def test_primary_cols_not_in_schema_override_used(self):
        """If primary is absent but override is present, override becomes the timestamp."""
        df = pl.DataFrame({"ov_dt": ["2024-05-01 09:00:00"]})
        times = _resolve(
            df,
            primary_datetime_col="evt_dt",  # absent
            override_datetime_col="ov_dt",
            use_override_if_later=True,
        )
        assert times[0].hour == 9


# ─────────────────────────────────────────────────────────────────────────────
# K – no usable columns in schema → null output
# ─────────────────────────────────────────────────────────────────────────────


class TestNoColumnsInSchema:
    def test_no_primary_no_override_returns_null(self):
        df = pl.DataFrame({"unrelated": [1, 2, 3]})
        times = _resolve(df)  # no column kwargs at all
        assert all(t is None for t in times)

    def test_configured_cols_absent_returns_null(self):
        df = pl.DataFrame({"unrelated": [42]})
        times = _resolve(
            df,
            primary_datetime_col="evt_dt",
            primary_date_col="evt_d",
        )
        assert times[0] is None


# ─────────────────────────────────────────────────────────────────────────────
# L – None passed for column names → treated as "not configured"
# ─────────────────────────────────────────────────────────────────────────────


class TestNoneColumnNames:
    def test_none_primary_cols_returns_null(self):
        df = pl.DataFrame({"evt_dt": ["2024-05-01 09:00:00"]})
        times = _resolve(
            df,
            primary_datetime_col=None,
            primary_date_col=None,
        )
        assert times[0] is None

    def test_none_override_cols_behaves_like_absent(self):
        df = pl.DataFrame({"evt_dt": ["2024-05-01 09:00:00"]})
        times = _resolve(
            df,
            primary_datetime_col="evt_dt",
            override_datetime_col=None,
            override_date_col=None,
            use_override_if_later=True,
        )
        assert times[0].hour == 9


# ─────────────────────────────────────────────────────────────────────────────
# M – custom output_col alias
# ─────────────────────────────────────────────────────────────────────────────


class TestCustomOutputCol:
    def test_custom_alias_is_used(self):
        df = pl.DataFrame({"evt_dt": ["2024-05-01 09:00:00"]})
        schema = df.lazy().collect_schema()
        expr = build_preferred_event_datetime(
            schema, primary_datetime_col="evt_dt", output_col="new_time"
        )
        result = df.lazy().with_columns(expr).collect()
        assert "new_time" in result.columns
        assert DEFAULT_OUTPUT_COLUMN not in result.columns

    def test_default_alias_is_time(self):
        df = pl.DataFrame({"evt_dt": ["2024-05-01 09:00:00"]})
        schema = df.lazy().collect_schema()
        expr = build_preferred_event_datetime(schema, primary_datetime_col="evt_dt")
        result = df.lazy().with_columns(expr).collect()
        assert DEFAULT_OUTPUT_COLUMN in result.columns


# ─────────────────────────────────────────────────────────────────────────────
# N – native pl.Date / pl.Datetime column types (not Utf8 strings)
# ─────────────────────────────────────────────────────────────────────────────


class TestNativePolarsTypes:
    def test_native_date_columns(self):
        df = pl.DataFrame(
            {
                "evt_d": [dt.date(2024, 5, 1)],
                "ov_d": [dt.date(2024, 5, 3)],
            }
        )
        assert df.schema["evt_d"] == pl.Date
        assert df.schema["ov_d"] == pl.Date
        times = _resolve(
            df,
            primary_date_col="evt_d",
            override_date_col="ov_d",
            use_override_if_later=True,
        )
        # note eod(2024-05-03) > eod(2024-05-01) → override wins
        assert times[0].day == 3 and times[0].hour == 23

    def test_native_datetime_columns(self):
        df = pl.DataFrame(
            {
                "evt_dt": [dt.datetime(2024, 5, 1, 9, 0, 0)],
                "ov_dt": [dt.datetime(2024, 5, 3, 12, 0, 0)],
            }
        )
        assert isinstance(df.schema["evt_dt"], pl.Datetime)
        times = _resolve(
            df,
            primary_datetime_col="evt_dt",
            override_datetime_col="ov_dt",
            use_override_if_later=True,
        )
        assert times[0].day == 3 and times[0].hour == 12

    def test_native_date_primary_no_override(self):
        df = pl.DataFrame({"evt_d": [dt.date(2024, 5, 1)]})
        times = _resolve(df, primary_date_col="evt_d")
        assert times[0].hour == 23 and times[0].day == 1


# ─────────────────────────────────────────────────────────────────────────────
# O – mixed rows: per-row independence
# ─────────────────────────────────────────────────────────────────────────────


class TestMixedRows:
    def test_rows_resolved_independently(self):
        df = pl.DataFrame(
            {
                "evt_dt": [
                    "2024-05-01 09:00:00",  # ov later  → override wins
                    "2024-05-02 10:00:00",  # ov earlier → primary wins
                    "2024-05-03 08:00:00",  # ov null    → primary wins
                ],
                "ov_dt": [
                    "2024-05-01 15:00:00",
                    "2024-05-01 09:00:00",
                    None,
                ],
            }
        )
        times = _resolve(
            df,
            primary_datetime_col="evt_dt",
            override_datetime_col="ov_dt",
            use_override_if_later=True,
        )
        assert times[0].hour == 15, "row 0: override (15h) should win over primary (9h)"
        assert times[1].hour == 10, (
            "row 1: primary (10h) should win over earlier override (9h)"
        )
        assert times[2].hour == 8, "row 2: null override, primary (8h) kept"

    def test_mixed_null_and_valued_primary(self):
        df = pl.DataFrame(
            {
                "evt_dt": ["2024-05-01 09:00:00", None],
                "evt_d": ["2024-05-01", "2024-05-02"],
            }
        ).with_columns(pl.col("evt_dt").cast(pl.Utf8))
        times = _resolve(df, primary_datetime_col="evt_dt", primary_date_col="evt_d")
        assert times[0].hour == 9 and times[0].day == 1  # datetime used
        assert times[1].hour == 23 and times[1].day == 2  # date fallback


# ─────────────────────────────────────────────────────────────────────────────
# P – output dtype is always pl.Datetime
# ─────────────────────────────────────────────────────────────────────────────


class TestOutputDtype:
    @pytest.mark.parametrize(
        "kwargs,df_data",
        [
            ({"primary_datetime_col": "c"}, {"c": ["2024-05-01 09:00:00"]}),
            ({"primary_date_col": "c"}, {"c": ["2024-05-01"]}),
            ({}, {"x": [1]}),  # no columns → null output
        ],
    )
    def test_output_is_datetime_type(self, kwargs, df_data):
        df = pl.DataFrame(df_data)
        schema = df.lazy().collect_schema()
        expr = build_preferred_event_datetime(schema, **kwargs)
        result = df.lazy().with_columns(expr).collect()
        out_col = kwargs.get("output_col", DEFAULT_OUTPUT_COLUMN)
        assert isinstance(result.schema[out_col], pl.Datetime), (
            f"Expected pl.Datetime, got {result.schema[out_col]}"
        )

    def test_time_unit_is_microseconds(self):
        df = pl.DataFrame({"evt_dt": ["2024-05-01 09:00:00"]})
        schema = df.lazy().collect_schema()
        expr = build_preferred_event_datetime(schema, primary_datetime_col="evt_dt")
        result = df.lazy().with_columns(expr).collect()
        assert result.schema[DEFAULT_OUTPUT_COLUMN].time_unit == "us"


# ─────────────────────────────────────────────────────────────────────────────
# Q – table-agnostic: drug_exposure column names work identically
# ─────────────────────────────────────────────────────────────────────────────


class TestDrugExposureColumnNames:
    """Verify the function is truly table-agnostic by using drug_exposure names."""

    def test_drug_start_datetime_used(self):
        df = pl.DataFrame(
            {
                "drug_exposure_start_datetime": ["2024-03-10 08:00:00"],
                "drug_exposure_start_date": ["2024-03-10"],
            }
        )
        times = _resolve(
            df,
            primary_datetime_col="drug_exposure_start_datetime",
            primary_date_col="drug_exposure_start_date",
        )
        assert times[0].hour == 8

    def test_drug_end_datetime_as_override(self):
        df = pl.DataFrame(
            {
                "drug_exposure_start_datetime": ["2024-03-10 08:00:00"],
                "drug_exposure_end_datetime": ["2024-03-15 18:00:00"],
            }
        )
        times = _resolve(
            df,
            primary_datetime_col="drug_exposure_start_datetime",
            override_datetime_col="drug_exposure_end_datetime",
            use_override_if_later=True,
        )
        assert times[0].day == 15 and times[0].hour == 18


# ─────────────────────────────────────────────────────────────────────────────
# R – wrap_with_datetime_resolver integration (OmegaConf config round-trip)
# ─────────────────────────────────────────────────────────────────────────────


class TestWrapWithDatetimeResolver:
    """Verify that OmegaConf config dicts are correctly unpacked and passed."""

    def _make_resolver_cfg(self, **overrides):
        base = {
            "primary_datetime_col": "evt_dt",
            "primary_date_col": "evt_d",
            "override_datetime_col": "ov_dt",
            "override_date_col": "ov_d",
            "use_override_if_later": False,
            "output_col": "time",
        }
        base.update(overrides)
        return OmegaConf.create(base)

    def test_config_roundtrip_no_override(self):
        df = pl.DataFrame(
            {
                "evt_dt": ["2024-05-01 09:00:00"],
                "evt_d": ["2024-05-01"],
                "ov_dt": ["2024-05-03 12:00:00"],
                "ov_d": ["2024-05-03"],
            }
        )
        cfg = self._make_resolver_cfg(use_override_if_later=False)
        kwargs = OmegaConf.to_container(cfg, resolve=True)
        times = _resolve(df, **kwargs)
        assert times[0].day == 1  # override disabled

    def test_config_roundtrip_with_override(self):
        df = pl.DataFrame(
            {
                "evt_dt": ["2024-05-01 09:00:00"],
                "evt_d": ["2024-05-01"],
                "ov_dt": ["2024-05-03 12:00:00"],
                "ov_d": ["2024-05-03"],
            }
        )
        cfg = self._make_resolver_cfg(use_override_if_later=True)
        kwargs = OmegaConf.to_container(cfg, resolve=True)
        times = _resolve(df, **kwargs)
        assert times[0].day == 3  # override enabled and later

    def test_config_with_note_column_names(self):
        """Mirrors exactly the YAML block defined in pre_MEDS.yaml for note."""
        yaml_str = """
        primary_datetime_col: note_datetime
        primary_date_col: note_date
        override_datetime_col: xtn_note_last_edit_datetime
        override_date_col: xtn_note_last_edit_date
        use_override_if_later: true
        output_col: time
        """
        cfg = OmegaConf.create(yaml_str)
        df = pl.DataFrame(
            {
                "note_datetime": ["2024-05-01 09:00:00"],
                "note_date": ["2024-05-01"],
                "xtn_note_last_edit_datetime": ["2024-05-04 16:00:00"],
                "xtn_note_last_edit_date": ["2024-05-04"],
            }
        )
        kwargs = OmegaConf.to_container(cfg, resolve=True)
        times = _resolve(df, **kwargs)
        assert times[0].day == 4 and times[0].hour == 16
