import pandas as pd
import numpy as np
from datetime import datetime
import os
import uuid

def generate_flight_data(records=100):
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
    df.loc[df.sample(frac=0.05).index, 'fuel_level_percentage'] = -10.0
    
    return df

if __name__ == "__main__":
    df = generate_flight_data(50)
    os.makedirs('data', exist_ok=True)
    filename = f"data/flights_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(filename, index=False)
    print(f"Generated {len(df)} records at {filename}")