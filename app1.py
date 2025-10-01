import pandas as pd
import mysql.connector

# --- MySQL connection ---
conn = mysql.connector.connect(
    host="localhost",
    user="flightuser",
    password="admin",
    database="flightdb"
)
cursor = conn.cursor()

# -----------------------
# 1. Import airlines.csv
# -----------------------
airlines_file = r"airlines.csv"
df_airlines = pd.read_csv(airlines_file)

# Replace NaN with None for SQL
df_airlines = df_airlines.where(pd.notnull(df_airlines), None)

# Insert data into airlines table
airlines_sql = "INSERT INTO airlines (iata_code, airline) VALUES (%s, %s)"
airlines_data = [tuple(row) for row in df_airlines.to_numpy()]
cursor.executemany(airlines_sql, airlines_data)
conn.commit()
print(f"Inserted {cursor.rowcount} rows into airlines table.")

# -----------------------
# 2. Import airports.csv
# -----------------------
airports_file = r"airports.csv"
df_airports = pd.read_csv(airports_file)

# Replace NaN with None for SQL
df_airports = df_airports.where(pd.notnull(df_airports), None)

# Insert data into airports table
airports_sql = """
INSERT INTO airports (iata_code, airport, city, state, country, latitude, longitude)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""
airports_data = [tuple(row) for row in df_airports.to_numpy()]
cursor.executemany(airports_sql, airports_data)
conn.commit()
print(f"Inserted {cursor.rowcount} rows into airports table.")

cursor.close()
conn.close()
print("All data imported successfully.")
