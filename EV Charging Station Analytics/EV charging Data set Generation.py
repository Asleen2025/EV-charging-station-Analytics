import csv
import pymysql

# ✅ Step 1: Connect to MySQL using pymysql
conn = pymysql.connect(
    host='localhost',
    user='root',
    password='Asleen@2504',
    database='EV_charge',
    charset='utf8mb4'
)
cursor = conn.cursor()

# ✅ Step 2: Open CSV and insert rows
with open(r"C:\\Users\\Ajay asleen\\OneDrive\\Documents\\Data Analytics Project\\EV Charging Station Utilization Analytics\\weather.csv",
          mode='r', encoding='utf-8') as file:
    csv_reader = csv.reader(file)
    next(csv_reader)  # Skip header row

    for row in csv_reader:
        cursor.execute("""
                INSERT IGNORE INTO weather ( Weather_ID,Timestamp,Location,Temperature_C,Rainfall_mm,Wind_Speed_kph,Humidity_pct,Weather_Condition)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, row)
# -------------------------------
# ✅ Step 3: Define CSV files & tables
# -------------------------------
datasets = [
    {
        "file": r"C:\Users\Ajay asleen\OneDrive\Documents\Data Analytics Project\EV Charging Station Utilization Analytics\stations.csv",
        "table": "stations",
        "columns": "(Station_ID, Station_Name, Location, Power_Level, Manufacturer)",
    },
    {
        "file": r"C:\Users\Ajay asleen\OneDrive\Documents\Data Analytics Project\EV Charging Station Utilization Analytics\vehicles.csv",
        "table": "vehicles",
        "columns": "(Vehicle_ID, Vehicle_Name, Manufacturer, Type)",
    },
    {
        "file": r"C:\Users\Ajay asleen\OneDrive\Documents\Data Analytics Project\EV Charging Station Utilization Analytics\charging_sessions.csv",
        "table": "charging_sessions",
        "columns": "(Session_ID, Station_ID, Vehicle_ID, Start_Time, End_Time, Duration_min, Revenue, Weather_Condition)",
    }
]

# -------------------------------
# ✅ Step 4: Read CSV and insert into table
# -------------------------------
for dataset in datasets:
    with open(dataset["file"], mode='r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header

        for row in csv_reader:
            # Construct INSERT query dynamically
            placeholders = ', '.join(['%s'] * len(row))
            query = f"""
                INSERT IGNORE INTO {dataset['table']} {dataset['columns']}
                VALUES ({placeholders})
            """
            cursor.execute(query, row)

# -------------------------------
# ✅ Step 5: Commit and close
# -------------------------------
conn.commit()
cursor.close()
conn.close()

print("✅ Data inserted successfully into all tables (duplicates ignored).")

