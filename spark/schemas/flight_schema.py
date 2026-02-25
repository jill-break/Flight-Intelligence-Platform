import pandera as pa
from pandera.typing import Series

class FlightSchema(pa.DataFrameModel):
    transaction_id: Series[str] = pa.Field(unique=True)
    flight_number: Series[str] = pa.Field(str_startswith="FL-")
    airline: Series[str]
    origin: Series[str] = pa.Field(str_length=3)
    destination: Series[str] = pa.Field(str_length=3)
    departure_time: Series[str]
    passenger_count: Series[int] = pa.Field(ge=0, le=850) # A380 capacity limit
    fuel_level_percentage: Series[float] = pa.Field(ge=0.0, le=100.0)
    is_delayed: Series[bool]

    class Config:
        strict = True  # Reject extra columns not defined here
        coerce = True  # Attempt to coerce types where possible