import pandas as pd
import numpy as np
from datetime import datetime
import os
import uuid

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
        df.loc[df.sample(frac=0.05).index, 'fuel_level_percentage'] = -10.0
    
    return df

if __name__ == "__main__":
    import sys
    import time

    count = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    continuous = len(sys.argv) > 2  # only loop if interval is explicitly provided
    interval = int(sys.argv[2]) if continuous else 30
    os.makedirs('data', exist_ok=True)

    CLEAN_COUNT = 10  # batches of clean data
    DIRTY_COUNT = 2   # batches of dirty data
    cycle_length = CLEAN_COUNT + DIRTY_COUNT  # 12 batches per cycle

    if continuous:
        print(f"✈️  Flight Generator started — {count} records every {interval}s")
        print(f"   Output: data/flights_*.csv")
        print(f"   Press Ctrl+C to stop\n")

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
        print(f"[Batch {batch}] [{label}] {len(df)} records ({dirty} dirty) -> {filename}")

        if not continuous:
            break  # single run for CI/CD
        time.sleep(interval)