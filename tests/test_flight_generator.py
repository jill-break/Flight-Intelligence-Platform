"""
Tests for scripts/flight_generator.py
"""
import sys
import os
import re
import pandas as pd
import pytest

# Ensure the project root is on sys.path so we can import scripts.*
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from scripts.flight_generator import generate_flight_data


# ---------- Structure Tests ----------

class TestGenerateFlightDataStructure:
    """Verify the shape and column layout of generated data."""

    def test_returns_dataframe(self):
        result = generate_flight_data(10)
        assert isinstance(result, pd.DataFrame)

    def test_correct_record_count(self):
        for n in [1, 50, 200]:
            df = generate_flight_data(n)
            assert len(df) == n, f"Expected {n} rows, got {len(df)}"

    def test_expected_columns(self):
        expected = [
            'transaction_id', 'flight_number', 'airline',
            'origin', 'destination', 'departure_time',
            'passenger_count', 'fuel_level_percentage', 'is_delayed',
        ]
        df = generate_flight_data(5)
        assert list(df.columns) == expected


# ---------- Data Quality Tests ----------

class TestGenerateFlightDataQuality:
    """Verify the content and constraints of generated data."""

    VALID_AIRLINES = {'Ghana Airways', 'British Airways', 'Delta', 'Emirates', 'JAL'}
    VALID_AIRPORTS = {'ACC', 'LHR', 'JFK', 'DXB', 'NRT'}

    def test_flight_number_format(self):
        df = generate_flight_data(100)
        pattern = re.compile(r'^FL-\d{3}$')
        assert df['flight_number'].apply(lambda x: bool(pattern.match(x))).all()

    def test_airline_values(self):
        df = generate_flight_data(100)
        assert set(df['airline'].unique()).issubset(self.VALID_AIRLINES)

    def test_airport_codes_length(self):
        df = generate_flight_data(100)
        assert (df['origin'].str.len() == 3).all()
        assert (df['destination'].str.len() == 3).all()

    def test_airport_codes_values(self):
        df = generate_flight_data(100)
        assert set(df['origin'].unique()).issubset(self.VALID_AIRPORTS)
        assert set(df['destination'].unique()).issubset(self.VALID_AIRPORTS)

    def test_passenger_count_range(self):
        df = generate_flight_data(200)
        assert (df['passenger_count'] >= 10).all()
        assert (df['passenger_count'] < 300).all()

    def test_dirty_data_injection(self):
        """~5% of records should have fuel_level_percentage == -10.0."""
        df = generate_flight_data(1000)
        dirty_count = (df['fuel_level_percentage'] == -10.0).sum()
        # Expect approximately 50 dirty records (5% of 1000)
        # Allow a tolerance range due to randomness
        assert 20 <= dirty_count <= 80, (
            f"Expected ~50 dirty records, got {dirty_count}"
        )

    def test_is_delayed_boolean(self):
        df = generate_flight_data(50)
        assert df['is_delayed'].dtype == bool

    def test_transaction_ids_are_unique(self):
        df = generate_flight_data(100)
        assert df['transaction_id'].is_unique
