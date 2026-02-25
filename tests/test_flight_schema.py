"""
Tests for spark/schemas/flight_schema.py — Pandera schema validation.
"""
import sys
import os
import pandas as pd
import pytest
import pandera

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from spark.schemas.flight_schema import FlightSchema


# ---------- Positive Tests ----------

class TestFlightSchemaValid:
    """Verify that well-formed data passes validation."""

    def test_valid_data_passes(self, valid_flight_df):
        validated = FlightSchema.validate(valid_flight_df)
        assert len(validated) == len(valid_flight_df)


# ---------- Negative Tests ----------

class TestFlightSchemaInvalid:
    """Verify that bad data correctly raises SchemaError."""

    def test_negative_fuel_fails(self, invalid_flight_df_negative_fuel):
        with pytest.raises(pandera.errors.SchemaError):
            FlightSchema.validate(invalid_flight_df_negative_fuel)

    def test_fuel_over_100_fails(self, valid_flight_df):
        df = valid_flight_df.copy()
        df.loc[0, 'fuel_level_percentage'] = 150.0
        with pytest.raises(pandera.errors.SchemaError):
            FlightSchema.validate(df)

    def test_negative_passengers_fails(self, valid_flight_df):
        df = valid_flight_df.copy()
        df.loc[0, 'passenger_count'] = -5
        with pytest.raises(pandera.errors.SchemaError):
            FlightSchema.validate(df)

    def test_passengers_over_850_fails(self, valid_flight_df):
        df = valid_flight_df.copy()
        df.loc[0, 'passenger_count'] = 900
        with pytest.raises(pandera.errors.SchemaError):
            FlightSchema.validate(df)

    def test_missing_column_fails(self, invalid_flight_df_missing_column):
        with pytest.raises(pandera.errors.SchemaError):
            FlightSchema.validate(invalid_flight_df_missing_column)

    def test_bad_flight_number_fails(self, invalid_flight_df_bad_flight_number):
        with pytest.raises(pandera.errors.SchemaError):
            FlightSchema.validate(invalid_flight_df_bad_flight_number)

    def test_duplicate_transaction_id_fails(self, invalid_flight_df_duplicate_ids):
        with pytest.raises(pandera.errors.SchemaError):
            FlightSchema.validate(invalid_flight_df_duplicate_ids)

    def test_airport_code_wrong_length_fails(self, invalid_flight_df_bad_airport_code):
        with pytest.raises(pandera.errors.SchemaError):
            FlightSchema.validate(invalid_flight_df_bad_airport_code)
