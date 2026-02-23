"""
Shared pytest fixtures for the Flight Intelligence Platform test suite.
"""
import pytest
import pandas as pd
import os
import tempfile


@pytest.fixture
def valid_flight_df():
    """A clean DataFrame that passes FlightSchema validation."""
    return pd.DataFrame({
        'transaction_id': ['id-001', 'id-002', 'id-003'],
        'flight_number': ['FL-101', 'FL-202', 'FL-303'],
        'airline': ['Ghana Airways', 'Delta', 'Emirates'],
        'origin': ['ACC', 'JFK', 'DXB'],
        'destination': ['LHR', 'NRT', 'ACC'],
        'departure_time': [
            '2026-01-15T10:00:00',
            '2026-01-15T12:00:00',
            '2026-01-15T14:00:00',
        ],
        'passenger_count': [150, 200, 300],
        'fuel_level_percentage': [85.5, 60.0, 99.9],
        'is_delayed': [False, True, False],
    })


@pytest.fixture
def invalid_flight_df_negative_fuel(valid_flight_df):
    """DataFrame with a negative fuel level — should fail schema validation."""
    df = valid_flight_df.copy()
    df.loc[0, 'fuel_level_percentage'] = -10.0
    return df


@pytest.fixture
def invalid_flight_df_missing_column(valid_flight_df):
    """DataFrame missing the 'airline' column."""
    return valid_flight_df.drop(columns=['airline'])


@pytest.fixture
def invalid_flight_df_duplicate_ids(valid_flight_df):
    """DataFrame with duplicate transaction IDs."""
    df = valid_flight_df.copy()
    df.loc[1, 'transaction_id'] = df.loc[0, 'transaction_id']
    return df


@pytest.fixture
def invalid_flight_df_bad_flight_number(valid_flight_df):
    """DataFrame with a flight number that doesn't start with FL-."""
    df = valid_flight_df.copy()
    df.loc[0, 'flight_number'] = 'XX-999'
    return df


@pytest.fixture
def invalid_flight_df_bad_airport_code(valid_flight_df):
    """DataFrame with an origin code that is not 3 characters."""
    df = valid_flight_df.copy()
    df.loc[0, 'origin'] = 'LONG'
    return df


@pytest.fixture
def sample_csv_path(valid_flight_df):
    """Write a valid DataFrame to a temporary CSV and yield the path."""
    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.csv', delete=False, newline=''
    ) as f:
        valid_flight_df.to_csv(f, index=False)
        path = f.name
    yield path
    os.unlink(path)
