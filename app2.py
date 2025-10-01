import pandas as pd
import mysql.connector
from mysql.connector import Error

# ---------- CONFIG ----------
flights_file = r"flights.csv"
DB_CONFIG = {
    "host": "localhost",
    "user": "flightuser",
    "password": "admin",
    "database": "flightdb",
    "autocommit": False
}
batch_size = 1000
# ----------------------------

# Exact 34 column names (in the same order as your CREATE TABLE)
TABLE_COLS = [
    "year", "month", "day", "day_of_week", "airline", "flight_number", "tail_number",
    "origin_airport", "destination_airport", "scheduled_departure", "departure_time",
    "departure_delay", "taxi_out", "wheels_off", "scheduled_time", "elapsed_time",
    "air_time", "distance", "wheels_on", "taxi_in", "scheduled_arrival", "arrival_time",
    "arrival_delay", "diverted", "cancelled", "cancellation_reason", "air_system_delay",
    "security_delay", "airline_delay", "late_aircraft_delay", "weather_delay",
    "flight_date", "scheduled_departure_time", "scheduled_arrival_time"
]

def load_and_insert():
    # Read CSV (everything as string initially)
    df = pd.read_csv(flights_file, dtype=str)

    # Convert headers to lowercase to match SQL table
    df.columns = df.columns.str.lower()

    # Drop completely empty columns (caused by trailing commas)
    df = df.dropna(axis=1, how="all")

    # Replace NaN with None (so MySQL stores NULL)
    df = df.where(pd.notnull(df), None)

    # Connect to MySQL
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        placeholders = ", ".join(["%s"] * len(TABLE_COLS))
        cols_sql = ", ".join(TABLE_COLS)
        insert_sql = f"INSERT INTO flights ({cols_sql}) VALUES ({placeholders})"

        buffer = []
        total_inserted = 0
        for _, row in df.iterrows():
            # Build tuple in the same order as TABLE_COLS
            values = tuple(row[col] if col in df.columns else None for col in TABLE_COLS)
            buffer.append(values)

            # Insert in batches
            if len(buffer) >= batch_size:
                cursor.executemany(insert_sql, buffer)
                conn.commit()
                total_inserted += len(buffer)
                print(f"Inserted {total_inserted} rows...")
                buffer = []

        # Insert remaining rows
        if buffer:
            cursor.executemany(insert_sql, buffer)
            conn.commit()
            total_inserted += len(buffer)
            print(f"Inserted {total_inserted} rows (final).")

        print("✅ flights.csv uploaded successfully.")
    except Error as e:
        conn.rollback()
        print("❌ ERROR:", e)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    load_and_insert()
