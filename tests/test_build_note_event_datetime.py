"""Tests for build_note_event_datetime in pre_meds_utils.py.

The function resolves a single canonical event datetime for the OMOP note table by:
  1. Preferring note_datetime over note_date (date is promoted to end-of-day).
  2. Optionally (use_last_edit_if_later=True) replacing that timestamp with the
     xtn_note_last_edit_datetime / xtn_note_last_edit_date value when it is
     non-null AND strictly later than the note timestamp.
"""

import polars as pl
import datetime as dt

from OMOP_MEDS.pre_meds_utils import build_note_event_datetime


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve(df: pl.DataFrame, **kwargs) -> list:
    """Collect the 'time' column produced by build_note_event_datetime."""
    schema = df.lazy().collect_schema()
    expr = build_note_event_datetime(schema, **kwargs)
    return df.lazy().with_columns(expr).collect()["time"].to_list()


# ---------------------------------------------------------------------------
# 1. Only note_datetime present
# ---------------------------------------------------------------------------


def test_note_datetime_used_when_present():
    df = pl.DataFrame(
        {
            "note_datetime": ["2024-05-01 09:00:00", "2024-06-15 14:30:00"],
            "note_date": ["2024-05-01", "2024-06-15"],
        }
    )
    times = _resolve(df)
    assert (
        times[0]
        == pl.Series("time", ["2024-05-01 09:00:00"]).str.to_datetime(
            "%Y-%m-%d %H:%M:%S"
        )[0]
    )
    assert (
        times[1]
        == pl.Series("time", ["2024-06-15 14:30:00"]).str.to_datetime(
            "%Y-%m-%d %H:%M:%S"
        )[0]
    )


def test_note_datetime_preferred_over_note_date():
    """note_datetime must win even when note_date is also populated."""
    df = pl.DataFrame(
        {
            "note_datetime": ["2024-05-01 09:00:00"],
            "note_date": ["2024-05-02"],  # later date, but datetime should win
        }
    )
    times = _resolve(df)
    assert times[0].hour == 9
    assert times[0].day == 1


# ---------------------------------------------------------------------------
# 2. note_date fallback → end of day
# ---------------------------------------------------------------------------


def test_note_date_promoted_to_end_of_day_when_no_datetime():
    df = pl.DataFrame(
        {
            "note_date": ["2024-05-01", "2024-06-15"],
        }
    )
    times = _resolve(df)
    assert times[0].hour == 23
    assert times[0].minute == 59
    assert times[0].second == 59
    assert times[0].day == 1
    assert times[1].month == 6
    assert times[1].day == 15


def test_note_date_null_datetime_falls_back_to_date():
    """When note_datetime is null but note_date is set, use note_date at end-of-day."""
    df = pl.DataFrame(
        {
            "note_datetime": [None],
            "note_date": ["2024-05-01"],
        }
    ).with_columns(pl.col("note_datetime").cast(pl.Utf8))
    times = _resolve(df)
    assert times[0] is not None
    assert times[0].hour == 23
    assert times[0].day == 1


# ---------------------------------------------------------------------------
# 3. Last-edit disabled (use_last_edit_if_later=False, the default)
# ---------------------------------------------------------------------------


def test_last_edit_ignored_when_disabled():
    """XTN columns must have no effect when use_last_edit_if_later=False."""
    df = pl.DataFrame(
        {
            "note_datetime": ["2024-05-01 09:00:00"],
            "note_date": ["2024-05-01"],
            "xtn_note_last_edit_datetime": ["2024-05-02 10:00:00"],  # later
            "xtn_note_last_edit_date": ["2024-05-02"],
        }
    )
    times = _resolve(df, use_last_edit_if_later=False)
    assert times[0].day == 1
    assert times[0].hour == 9


# ---------------------------------------------------------------------------
# 4. Last-edit enabled and xtn datetime is later
# ---------------------------------------------------------------------------


def test_last_edit_datetime_replaces_note_datetime_when_later():
    df = pl.DataFrame(
        {
            "note_datetime": ["2024-05-01 09:00:00"],
            "note_date": ["2024-05-01"],
            "xtn_note_last_edit_datetime": ["2024-05-03 12:00:00"],
            "xtn_note_last_edit_date": ["2024-05-03"],
        }
    )
    times = _resolve(df, use_last_edit_if_later=True)
    assert times[0].day == 3
    assert times[0].hour == 12


def test_last_edit_datetime_does_not_replace_when_earlier():
    """XTN value that predates the note timestamp must be ignored."""
    df = pl.DataFrame(
        {
            "note_datetime": ["2024-05-05 14:00:00"],
            "note_date": ["2024-05-05"],
            "xtn_note_last_edit_datetime": ["2024-05-01 08:00:00"],
            "xtn_note_last_edit_date": ["2024-05-01"],
        }
    )
    times = _resolve(df, use_last_edit_if_later=True)
    assert times[0].day == 5
    assert times[0].hour == 14


def test_last_edit_datetime_does_not_replace_when_equal():
    """XTN value equal to note timestamp is not 'later'; original is kept."""
    ts = "2024-05-01 09:00:00"
    df = pl.DataFrame(
        {
            "note_datetime": [ts],
            "note_date": ["2024-05-01"],
            "xtn_note_last_edit_datetime": [ts],
            "xtn_note_last_edit_date": ["2024-05-01"],
        }
    )
    times = _resolve(df, use_last_edit_if_later=True)
    assert times[0].hour == 9
    assert times[0].day == 1


# ---------------------------------------------------------------------------
# 5. Last-edit enabled but xtn datetime is null → fall back to xtn date
# ---------------------------------------------------------------------------


def test_last_edit_date_fallback_when_edit_datetime_null():
    """If xtn_note_last_edit_datetime is null, xtn_note_last_edit_date is used at end-of-day."""
    df = pl.DataFrame(
        {
            "note_datetime": ["2024-05-01 09:00:00"],
            "note_date": ["2024-05-01"],
            "xtn_note_last_edit_datetime": [None],
            "xtn_note_last_edit_date": ["2024-05-03"],  # 2024-05-03 23:59:59 > note
        }
    ).with_columns(pl.col("xtn_note_last_edit_datetime").cast(pl.Utf8))
    times = _resolve(df, use_last_edit_if_later=True)
    assert times[0].day == 3
    assert times[0].hour == 23
    assert times[0].second == 59


def test_last_edit_date_fallback_earlier_than_note_is_ignored():
    df = pl.DataFrame(
        {
            "note_datetime": ["2024-05-05 23:00:00"],
            "note_date": ["2024-05-05"],
            "xtn_note_last_edit_datetime": [None],
            "xtn_note_last_edit_date": [
                "2024-05-05"
            ],  # end-of-day is 23:59:59, still later
        }
    ).with_columns(pl.col("xtn_note_last_edit_datetime").cast(pl.Utf8))
    # note_datetime = 23:00:00, edit = 23:59:59 on the same day → edit wins
    times = _resolve(df, use_last_edit_if_later=True)
    assert times[0].hour == 23
    assert times[0].second == 59


# ---------------------------------------------------------------------------
# 6. Both xtn columns null → note timestamp unchanged
# ---------------------------------------------------------------------------


def test_null_xtn_columns_do_not_affect_result():
    df = pl.DataFrame(
        {
            "note_datetime": ["2024-05-01 09:00:00"],
            "note_date": ["2024-05-01"],
            "xtn_note_last_edit_datetime": [None],
            "xtn_note_last_edit_date": [None],
        }
    ).with_columns(
        [
            pl.col("xtn_note_last_edit_datetime").cast(pl.Utf8),
            pl.col("xtn_note_last_edit_date").cast(pl.Utf8),
        ]
    )
    times = _resolve(df, use_last_edit_if_later=True)
    assert times[0].day == 1
    assert times[0].hour == 9


# ---------------------------------------------------------------------------
# 7. XTN columns absent from schema entirely
# ---------------------------------------------------------------------------


def test_missing_xtn_columns_use_note_timestamp():
    """Schema without XTN columns must work cleanly (no KeyError)."""
    df = pl.DataFrame(
        {
            "note_datetime": ["2024-05-01 09:00:00"],
            "note_date": ["2024-05-01"],
        }
    )
    times = _resolve(df, use_last_edit_if_later=True)
    assert times[0].hour == 9


# ---------------------------------------------------------------------------
# 8. Mixed rows – some edits later, some not, some null
# ---------------------------------------------------------------------------


def test_mixed_rows_with_last_edit():
    df = pl.DataFrame(
        {
            "note_datetime": [
                "2024-05-01 09:00:00",  # edit later  → expect edit
                "2024-05-02 10:00:00",  # edit earlier → expect note
                "2024-05-03 08:00:00",  # edit null    → expect note
            ],
            "note_date": ["2024-05-01", "2024-05-02", "2024-05-03"],
            "xtn_note_last_edit_datetime": [
                "2024-05-01 15:00:00",
                "2024-05-01 09:00:00",
                None,
            ],
            "xtn_note_last_edit_date": ["2024-05-01", "2024-05-01", None],
        }
    )
    times = _resolve(df, use_last_edit_if_later=True)
    assert times[0].hour == 15, "Row 0: edit (15h) should replace note (9h)"
    assert times[1].hour == 10, (
        "Row 1: note (10h) should be kept over earlier edit (9h)"
    )
    assert times[2].hour == 8, "Row 2: null edit, note (8h) kept"


# ---------------------------------------------------------------------------
# 9. Output dtype is always pl.Datetime
# ---------------------------------------------------------------------------


def test_output_dtype_is_datetime():
    df = pl.DataFrame(
        {
            "note_datetime": ["2024-05-01 09:00:00"],
            "note_date": ["2024-05-01"],
        }
    )
    schema = df.lazy().collect_schema()
    expr = build_note_event_datetime(schema)
    result = df.lazy().with_columns(expr).collect()
    assert isinstance(result.schema["time"], pl.Datetime), (
        f"Expected Datetime, got {result.schema['time']}"
    )


# ---------------------------------------------------------------------------
# 10. polars Date-typed columns (not string)
# ---------------------------------------------------------------------------


def test_native_date_type_columns():
    """Columns with native pl.Date / pl.Datetime types (not Utf8) must work."""
    df = pl.DataFrame(
        {
            "note_date": [dt.date(2024, 5, 1)],
            "xtn_note_last_edit_date": [dt.date(2024, 5, 3)],
        }
    )
    times = _resolve(df, use_last_edit_if_later=True)
    assert times[0].day == 3
    assert times[0].month == 5
    assert times[0].year == 2024


def test_native_datetime_type_columns():
    import datetime as dt

    df = pl.DataFrame(
        {
            "note_datetime": [dt.datetime(2024, 5, 1, 9, 0, 0)],
            "xtn_note_last_edit_datetime": [dt.datetime(2024, 5, 3, 12, 0, 0)],
        }
    )
    times = _resolve(df, use_last_edit_if_later=True)
    assert times[0].day == 3
    assert times[0].hour == 12
