"""
Tests for airflow/dags/quarantine_cleaner.py
=============================================
Verifies the cleaning pipeline handles all real-world error patterns:
  - Numeric clamping (fuel, passengers)
  - Whitespace stripping
  - Invalid UUID transaction IDs
  - Logical inconsistencies (origin == destination)
  - Duplicate transaction IDs
  - Invalid flight numbers and airport codes
  - Null values in required columns
  - Dropped-row report format
"""

import sys
import os
import pandas as pd
import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'airflow', 'dags'))

from quarantine_cleaner import clean_dataframe, build_dropped_rows_report


# ──────────────────────────────────────────────
# Numeric Clamping Tests
# ──────────────────────────────────────────────

class TestNumericClamping:
    """Verify that out-of-range numeric values are clamped, not dropped."""

    def test_negative_fuel_clamped_to_zero(self, valid_flight_df):
        df = valid_flight_df.copy()
        df.loc[0, 'fuel_level_percentage'] = -10.0
        cleaned, dropped = clean_dataframe(df, 'test.csv')
        assert (cleaned['fuel_level_percentage'] >= 0.0).all()
        assert cleaned.loc[cleaned.index[0], 'fuel_level_percentage'] == 0.0

    def test_fuel_over_100_clamped(self, valid_flight_df):
        df = valid_flight_df.copy()
        df.loc[0, 'fuel_level_percentage'] = 150.0
        cleaned, dropped = clean_dataframe(df, 'test.csv')
        assert (cleaned['fuel_level_percentage'] <= 100.0).all()
        assert cleaned.loc[cleaned.index[0], 'fuel_level_percentage'] == 100.0

    def test_negative_passengers_clamped(self, valid_flight_df):
        df = valid_flight_df.copy()
        df.loc[0, 'passenger_count'] = -5
        cleaned, dropped = clean_dataframe(df, 'test.csv')
        assert (cleaned['passenger_count'] >= 0).all()

    def test_passengers_over_850_clamped(self, valid_flight_df):
        df = valid_flight_df.copy()
        df.loc[0, 'passenger_count'] = 900
        cleaned, dropped = clean_dataframe(df, 'test.csv')
        assert (cleaned['passenger_count'] <= 850).all()
        assert cleaned.loc[cleaned.index[0], 'passenger_count'] == 850


# ──────────────────────────────────────────────
# Whitespace Stripping Tests
# ──────────────────────────────────────────────

class TestWhitespaceStripping:
    """Verify whitespace is stripped from string columns."""

    def test_leading_trailing_whitespace_stripped(self, valid_flight_df):
        df = valid_flight_df.copy()
        df.loc[0, 'origin'] = '  ACC  '
        df.loc[0, 'airline'] = '  Ghana Airways  '
        cleaned, dropped = clean_dataframe(df, 'test.csv')
        assert cleaned.loc[cleaned.index[0], 'origin'] == 'ACC'
        assert cleaned.loc[cleaned.index[0], 'airline'] == 'Ghana Airways'


# ──────────────────────────────────────────────
# Transaction ID Validation Tests
# ──────────────────────────────────────────────

class TestTransactionIdValidation:
    """Verify invalid transaction IDs are dropped and tracked."""

    def test_invalid_uuid_dropped(self, invalid_transaction_id_df):
        cleaned, dropped = clean_dataframe(invalid_transaction_id_df, 'test.csv')
        # All rows have invalid UUIDs — should all be dropped
        assert len(cleaned) == 0
        assert len(dropped) > 0

    def test_empty_string_transaction_id_dropped(self, valid_flight_df):
        df = valid_flight_df.copy()
        df.loc[0, 'transaction_id'] = ''
        cleaned, dropped = clean_dataframe(df, 'test.csv')
        # The empty-string row should be gone
        assert len(cleaned) < len(df)

    def test_non_uuid_format_dropped(self, valid_flight_df):
        df = valid_flight_df.copy()
        df.loc[0, 'transaction_id'] = 'not-a-valid-uuid'
        cleaned, dropped = clean_dataframe(df, 'test.csv')
        assert len(cleaned) < len(df)
        assert any('invalid_transaction_id' in str(r) for r in dropped['drop_reason'].values)


# ──────────────────────────────────────────────
# Logical Inconsistency Tests
# ──────────────────────────────────────────────

class TestLogicalInconsistencies:
    """Verify rows with origin == destination are dropped."""

    def test_same_origin_destination_dropped(self, inconsistent_flight_df):
        cleaned, dropped = clean_dataframe(inconsistent_flight_df, 'test.csv')
        # Rows 0 and 2 have origin == destination
        if len(cleaned) > 0:
            assert (cleaned['origin'] != cleaned['destination']).all()
        same_route_drops = dropped[
            dropped['drop_reason'].str.contains('origin == destination', na=False)
        ]
        assert len(same_route_drops) >= 2


# ──────────────────────────────────────────────
# Duplicate Transaction ID Tests
# ──────────────────────────────────────────────

class TestDeduplication:
    """Verify duplicate transaction IDs are deduplicated (keep first)."""

    def test_duplicate_ids_deduplicated(self, dirty_flight_df):
        cleaned, dropped = clean_dataframe(dirty_flight_df, 'test.csv')
        if 'transaction_id' in cleaned.columns and len(cleaned) > 0:
            assert cleaned['transaction_id'].is_unique
        dup_drops = dropped[
            dropped['drop_reason'].str.contains('duplicate_transaction_id', na=False)
        ]
        assert len(dup_drops) >= 1


# ──────────────────────────────────────────────
# Flight Number Validation Tests
# ──────────────────────────────────────────────

class TestFlightNumberValidation:
    """Verify invalid flight numbers are dropped."""

    def test_bad_flight_number_dropped(self, dirty_flight_df):
        cleaned, dropped = clean_dataframe(dirty_flight_df, 'test.csv')
        if len(cleaned) > 0:
            assert cleaned['flight_number'].str.match(r'^FL-\d{3}$').all()
        fn_drops = dropped[
            dropped['drop_reason'].str.contains('invalid_flight_number', na=False)
        ]
        assert len(fn_drops) >= 1


# ──────────────────────────────────────────────
# Airport Code Validation Tests
# ──────────────────────────────────────────────

class TestAirportCodeValidation:
    """Verify invalid airport codes (not 3 chars) are dropped."""

    def test_long_airport_code_dropped(self, valid_flight_df):
        df = valid_flight_df.copy()
        df.loc[0, 'origin'] = 'LONG'
        cleaned, dropped = clean_dataframe(df, 'test.csv')
        assert len(cleaned) < len(df)
        code_drops = dropped[
            dropped['drop_reason'].str.contains('invalid_airport_code', na=False)
        ]
        assert len(code_drops) >= 1


# ──────────────────────────────────────────────
# Null Value Tests
# ──────────────────────────────────────────────

class TestNullValues:
    """Verify rows with nulls in required columns are dropped."""

    def test_null_airline_dropped(self, valid_flight_df):
        df = valid_flight_df.copy()
        df.loc[0, 'airline'] = None
        cleaned, dropped = clean_dataframe(df, 'test.csv')
        assert len(cleaned) < len(df)
        null_drops = dropped[
            dropped['drop_reason'].str.contains('null_value', na=False)
        ]
        assert len(null_drops) >= 1


# ──────────────────────────────────────────────
# Dropped Rows Report Format Tests
# ──────────────────────────────────────────────

class TestDroppedRowsReport:
    """Verify the dropped-rows output has the correct format."""

    def test_dropped_report_columns(self, dirty_flight_df):
        _, dropped = clean_dataframe(dirty_flight_df, 'test.csv')
        report = build_dropped_rows_report(dropped, 'test.csv')
        assert list(report.columns) == ['source_filename', 'transaction_id', 'drop_reason']

    def test_dropped_report_source_filename(self, dirty_flight_df):
        _, dropped = clean_dataframe(dirty_flight_df, 'my_file.csv')
        report = build_dropped_rows_report(dropped, 'my_file.csv')
        assert (report['source_filename'] == 'my_file.csv').all()

    def test_empty_input_returns_empty(self):
        empty_df = pd.DataFrame(columns=[
            'transaction_id', 'flight_number', 'airline', 'origin',
            'destination', 'departure_time', 'passenger_count',
            'fuel_level_percentage', 'is_delayed',
        ])
        cleaned, dropped = clean_dataframe(empty_df, 'empty.csv')
        assert len(cleaned) == 0
        assert len(dropped) == 0

    def test_empty_dropped_report(self):
        empty_dropped = pd.DataFrame(columns=['source_filename', 'transaction_id', 'drop_reason'])
        report = build_dropped_rows_report(empty_dropped, 'test.csv')
        assert list(report.columns) == ['source_filename', 'transaction_id', 'drop_reason']
        assert len(report) == 0


# ──────────────────────────────────────────────
# Type Mismatch Tests
# ──────────────────────────────────────────────

class TestTypeMismatches:
    """Verify type mismatches in numeric columns are dropped."""

    def test_non_numeric_passenger_count_dropped(self, valid_flight_df):
        df = valid_flight_df.copy()
        df['passenger_count'] = df['passenger_count'].astype(object)
        df.loc[0, 'passenger_count'] = 'abc'
        cleaned, dropped = clean_dataframe(df, 'test.csv')
        assert len(cleaned) < len(df)
        type_drops = dropped[
            dropped['drop_reason'].str.contains('type_mismatch', na=False)
        ]
        assert len(type_drops) >= 1

    def test_non_numeric_fuel_dropped(self, valid_flight_df):
        df = valid_flight_df.copy()
        df['fuel_level_percentage'] = df['fuel_level_percentage'].astype(object)
        df.loc[0, 'fuel_level_percentage'] = 'invalid'
        cleaned, dropped = clean_dataframe(df, 'test.csv')
        assert len(cleaned) < len(df)
        type_drops = dropped[
            dropped['drop_reason'].str.contains('type_mismatch', na=False)
        ]
        assert len(type_drops) >= 1
