import pandas as pd
import numpy as np
from datetime import datetime
import os
import sys
import uuid
import logging

# ──────────────────────────────────────────────
# Logger Setup
# ──────────────────────────────────────────────
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger('flight_generator')
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
file_handler = logging.FileHandler(os.path.join(LOG_DIR, 'flight_generator.log'), encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def generate_flight_data(records=100, inject_dirty=True):
    airports = ['ACC', 'LHR', 'JFK', 'DXB', 'NRT']
    airlines = ['Ghana Airways', 'British Airways', 'Delta', 'Emirates', 'JAL']
    
    data = {
        'transaction_id': [str(uuid.uuid4()) for _ in range(records)],
        'flight_number': [f"FL-{np.random.randint(100, 999)}" for _ in range(records)],
        'airline': [np.random.choice(airlines) for _ in range(records)],
        'origin': [np.random.choice(airports) for _ in range(records)],
        'destination': [np.random.choice(airports) for _ in range(records)],
        'departure_time': [datetime.now().isoformat() for _ in range(records)],
        'passenger_count': [np.random.randint(10, 300) for _ in range(records)],
        'fuel_level_percentage': [np.random.uniform(5.0, 100.0) for _ in range(records)],
        'is_delayed': [np.random.choice([True, False]) for _ in range(records)]
    }
    
    df = pd.DataFrame(data)
    
    # Introduce "Dirty Data" for testing (5% of records have negative fuel)
    if inject_dirty:
        dirty_count = max(1, int(len(df) * 0.05))
        df.loc[df.sample(n=dirty_count).index, 'fuel_level_percentage'] = -10.0
        logger.debug(f"Injected {dirty_count} dirty records (negative fuel)")
    
    return df

if __name__ == "__main__":
    import time

    count = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    continuous = len(sys.argv) > 2  # only loop if interval is explicitly provided
    interval = int(sys.argv[2]) if continuous else 30
    os.makedirs('data', exist_ok=True)

    CLEAN_COUNT = 10  # batches of clean data
    DIRTY_COUNT = 2   # batches of dirty data
    cycle_length = CLEAN_COUNT + DIRTY_COUNT  # 12 batches per cycle

    if continuous:
        logger.info(f"Flight Generator started — {count} records every {interval}s")
        logger.info(f"Output directory: data/flights_*.csv")
        logger.info("Press Ctrl+C to stop")

    batch = 0
    while True:
        batch += 1
        position = batch % cycle_length

        # Batches 1-10 are clean, 11-12 are dirty
        inject_dirty = position > CLEAN_COUNT or position == 0
        label = "DIRTY" if inject_dirty else "CLEAN"

        df = generate_flight_data(count, inject_dirty=inject_dirty)
        filename = f"data/flights_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        dirty = len(df[df['fuel_level_percentage'] < 0])

        if inject_dirty:
            logger.warning(f"[Batch {batch}] [{label}] {len(df)} records ({dirty} dirty) -> {filename}")
        else:
            logger.info(f"[Batch {batch}] [{label}] {len(df)} records -> {filename}")

        if not continuous:
            break  # single run for CI/CD
        time.sleep(interval)