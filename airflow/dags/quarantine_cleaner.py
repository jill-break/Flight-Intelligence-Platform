"""
Quarantine Cleaner — Data Cleaning Logic
==========================================
Pure functions that clean quarantined flight data and track dropped rows.
Used by the flight_analytics_pipeline DAG to recover data from quarantine.

Cleaning Pipeline:
  1. Strip whitespace from string columns
  2. Coerce types (passenger_count → int, fuel_level_percentage → float)
  3. Validate transaction_id UUID format
  4. Clamp fuel_level_percentage to [0.0, 100.0]
  5. Clamp passenger_count to [0, 850]
  6. Drop rows where origin == destination
  7. Drop rows with future departure times (> 24h from now)
  8. Drop rows with null values in required columns
  9. Deduplicate on transaction_id (keep first)
  10. Final Pandera validation — drop remaining failures
"""

import pandas as pd
import numpy as np
import re
import logging
from datetime import datetime, timedelta

logger = logging.getLogger('quarantine_cleaner')

# UUID v4 pattern (loose — accepts any hex-formatted UUID)
UUID_PATTERN = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    re.IGNORECASE,
)

REQUIRED_COLUMNS = [
    'transaction_id', 'flight_number', 'airline', 'origin',
    'destination', 'departure_time', 'passenger_count',
    'fuel_level_percentage', 'is_delayed',
]

FLIGHT_NUMBER_PATTERN = re.compile(r'^FL-\d{3}$')


def _track(dropped_records, df_rows, filename, reason):
    """Append rows to the dropped-records list with a reason."""
    if df_rows.empty:
        return
    for _, row in df_rows.iterrows():
        dropped_records.append({
            'source_filename': filename,
            'transaction_id': str(row.get('transaction_id', 'UNKNOWN')),
            'drop_reason': reason,
        })


def clean_dataframe(df, filename):
    """
    Apply the full cleaning pipeline to a quarantined DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Raw data from a quarantined CSV file.
    filename : str
        The source filename (for tracking dropped rows).

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        (cleaned_df, dropped_df) where dropped_df has columns:
        source_filename, transaction_id, drop_reason
    """
    dropped_records = []
    working = df.copy()

    # Early return for empty DataFrames
    if working.empty:
        dropped_df = pd.DataFrame(columns=['source_filename', 'transaction_id', 'drop_reason'])
        return working, dropped_df

    # ── 1. Strip whitespace from all string columns ──────────────
    str_cols = working.select_dtypes(include=['object']).columns
    for col in str_cols:
        # Only apply string methods to actual strings, avoiding converting None to 'None'
        working[col] = working[col].apply(lambda x: str(x).strip() if pd.notna(x) else np.nan)
        # Also catch the case where a literal 'None' or 'nan' became a string
        working[col] = working[col].replace({'': np.nan, 'None': np.nan, 'nan': np.nan})

    # ── 2. Coerce numeric types ──────────────────────────────────
    # passenger_count
    original = working.copy()
    working['passenger_count'] = pd.to_numeric(
        working['passenger_count'], errors='coerce'
    )
    bad_passengers = working[working['passenger_count'].isna() & original['passenger_count'].notna()]
    _track(dropped_records, bad_passengers, filename, 'type_mismatch: passenger_count not numeric')
    working = working.drop(bad_passengers.index)

    # fuel_level_percentage
    original = working.copy()
    working['fuel_level_percentage'] = pd.to_numeric(
        working['fuel_level_percentage'], errors='coerce'
    )
    bad_fuel = working[working['fuel_level_percentage'].isna() & original['fuel_level_percentage'].notna()]
    _track(dropped_records, bad_fuel, filename, 'type_mismatch: fuel_level_percentage not numeric')
    working = working.drop(bad_fuel.index)

    # ── 3. Validate transaction_id UUID format ───────────────────
    if 'transaction_id' in working.columns:
        invalid_uuid_mask = ~working['transaction_id'].astype(str).apply(
            lambda x: bool(UUID_PATTERN.match(str(x)))
        )
        invalid_uuids = working[invalid_uuid_mask]
        _track(dropped_records, invalid_uuids, filename, 'invalid_transaction_id: not a valid UUID')
        working = working[~invalid_uuid_mask]

    # ── 4. Clamp fuel_level_percentage to [0.0, 100.0] ───────────
    fuel_clamped_low = (working['fuel_level_percentage'] < 0).sum()
    fuel_clamped_high = (working['fuel_level_percentage'] > 100).sum()
    working['fuel_level_percentage'] = working['fuel_level_percentage'].clip(0.0, 100.0)
    if fuel_clamped_low + fuel_clamped_high > 0:
        logger.info(
            f"[{filename}] Clamped fuel_level_percentage: "
            f"{fuel_clamped_low} low, {fuel_clamped_high} high"
        )

    # ── 5. Clamp passenger_count to [0, 850] ─────────────────────
    pax_clamped_low = (working['passenger_count'] < 0).sum()
    pax_clamped_high = (working['passenger_count'] > 850).sum()
    working['passenger_count'] = working['passenger_count'].clip(0, 850).astype(int)
    if pax_clamped_low + pax_clamped_high > 0:
        logger.info(
            f"[{filename}] Clamped passenger_count: "
            f"{pax_clamped_low} low, {pax_clamped_high} high"
        )

    # ── 6. Drop rows where origin == destination ─────────────────
    if 'origin' in working.columns and 'destination' in working.columns:
        same_route = working[working['origin'] == working['destination']]
        _track(dropped_records, same_route, filename, 'logical_inconsistency: origin == destination')
        working = working[working['origin'] != working['destination']]

    # ── 7. Drop rows with future departure times (> 24h) ─────────
    if 'departure_time' in working.columns:
        try:
            dt_parsed = pd.to_datetime(working['departure_time'], errors='coerce')
            future_cutoff = datetime.now() + timedelta(hours=24)
            future_mask = dt_parsed > future_cutoff
            future_rows = working[future_mask]
            _track(dropped_records, future_rows, filename, 'future_departure_time: > 24h from now')
            working = working[~future_mask]
        except Exception:
            logger.warning(f"[{filename}] Could not parse departure_time for future check")

    # ── 8. Drop rows with null values in required columns ────────
    for col in REQUIRED_COLUMNS:
        if col in working.columns:
            null_mask = working[col].isna()
            null_rows = working[null_mask]
            _track(dropped_records, null_rows, filename, f'null_value: {col} is null')
            working = working[~null_mask]

    # ── 9. Deduplicate on transaction_id ─────────────────────────
    if 'transaction_id' in working.columns:
        dup_mask = working['transaction_id'].duplicated(keep='first')
        dup_rows = working[dup_mask]
        _track(dropped_records, dup_rows, filename, 'duplicate_transaction_id')
        working = working[~dup_mask]

    # ── 10. Validate flight_number format ────────────────────────
    if 'flight_number' in working.columns:
        bad_fn_mask = ~working['flight_number'].astype(str).apply(
            lambda x: bool(FLIGHT_NUMBER_PATTERN.match(x))
        )
        bad_fn = working[bad_fn_mask]
        _track(dropped_records, bad_fn, filename, 'invalid_flight_number: does not match FL-XXX')
        working = working[~bad_fn_mask]

    # ── 11. Validate airport code length (3 chars) ───────────────
    for col in ['origin', 'destination']:
        if col in working.columns:
            bad_code_mask = working[col].astype(str).str.len() != 3
            bad_codes = working[bad_code_mask]
            _track(dropped_records, bad_codes, filename, f'invalid_airport_code: {col} not 3 chars')
            working = working[~bad_code_mask]

    # Build dropped DataFrame
    dropped_df = pd.DataFrame(dropped_records)
    if dropped_df.empty:
        dropped_df = pd.DataFrame(columns=['source_filename', 'transaction_id', 'drop_reason'])

    cleaned_count = len(working)
    dropped_count = len(dropped_df)
    logger.info(
        f"[{filename}] Cleaning complete: {cleaned_count} recovered, {dropped_count} dropped"
    )

    return working, dropped_df


def build_dropped_rows_report(dropped_df, filename):
    """
    Ensure the dropped-rows DataFrame has the correct output format.

    Parameters
    ----------
    dropped_df : pd.DataFrame
        Output from clean_dataframe.
    filename : str
        Source filename (already embedded in the DataFrame).

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: source_filename, transaction_id, drop_reason
    """
    if dropped_df.empty:
        return pd.DataFrame(columns=['source_filename', 'transaction_id', 'drop_reason'])

    return dropped_df[['source_filename', 'transaction_id', 'drop_reason']].copy()
