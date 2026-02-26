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
        'transaction_id': [
            '11111111-1111-1111-1111-111111111111',
            '22222222-2222-2222-2222-222222222222',
            '33333333-3333-3333-3333-333333333333',
        ],
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


# ──────────────────────────────────────────────
# Quarantine Cleaner Fixtures
# ──────────────────────────────────────────────

@pytest.fixture
def dirty_flight_df():
    """DataFrame with multiple types of dirty data for quarantine cleaning tests."""
    return pd.DataFrame({
        'transaction_id': [
            'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
            'b2c3d4e5-f6a7-8901-bcde-f12345678901',
            'c3d4e5f6-a7b8-9012-cdef-123456789012',
            'd4e5f6a7-b8c9-0123-defa-234567890123',
            'a1b2c3d4-e5f6-7890-abcd-ef1234567890',  # duplicate of row 0
        ],
        'flight_number': ['FL-101', 'FL-202', 'XX-999', 'FL-404', 'FL-505'],
        'airline': ['Ghana Airways', 'Delta', 'Emirates', 'JAL', 'Delta'],
        'origin': ['ACC', 'JFK', 'DXB', 'NRT', 'LHR'],
        'destination': ['LHR', 'NRT', 'ACC', 'LHR', 'JFK'],
        'departure_time': [
            '2026-01-15T10:00:00',
            '2026-01-15T12:00:00',
            '2026-01-15T14:00:00',
            '2026-01-15T16:00:00',
            '2026-01-15T18:00:00',
        ],
        'passenger_count': [150, -5, 200, 900, 100],
        'fuel_level_percentage': [-10.0, 60.0, 85.5, 99.9, 50.0],
        'is_delayed': [False, True, False, True, False],
    })


@pytest.fixture
def inconsistent_flight_df():
    """DataFrame with logical inconsistencies: origin==destination, future timestamps."""
    return pd.DataFrame({
        'transaction_id': [
            'e5f6a7b8-c9d0-1234-5678-90abcdef1234',
            'f6a7b8c9-d0e1-2345-6789-0abcdef12345',
            'a7b8c9d0-e1f2-3456-7890-abcdef123456',
        ],
        'flight_number': ['FL-101', 'FL-202', 'FL-303'],
        'airline': ['Ghana Airways', 'Delta', 'Emirates'],
        'origin': ['ACC', 'JFK', 'DXB'],
        'destination': ['ACC', 'NRT', 'DXB'],  # rows 0 and 2: origin == destination
        'departure_time': [
            '2026-01-15T10:00:00',
            '2099-12-31T23:59:59',  # far future
            '2026-01-15T14:00:00',
        ],
        'passenger_count': [150, 200, 300],
        'fuel_level_percentage': [85.5, 60.0, 99.9],
        'is_delayed': [False, True, False],
    })


@pytest.fixture
def invalid_transaction_id_df():
    """DataFrame with invalid transaction IDs: empty strings, non-UUID, None."""
    return pd.DataFrame({
        'transaction_id': ['', 'not-a-uuid', None],
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
