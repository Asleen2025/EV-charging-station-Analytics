EV Charging Analytics Project
=============================

A complete end-to-end data analytics solution for electric vehicle (EV) charging infrastructure. This project integrates Python-based data generation, SQL-based relational modeling, and Power BI dashboarding to uncover insights into station utilization, vehicle behavior, and weather impact.

Project Overview
----------------
This repository showcases:
- Synthetic data generation using Python
- Relational schema design and SQL queries for EV charging sessions
- Interactive Power BI dashboard with KPIs and visual storytelling
- CSV outputs for peak hours, station performance, vehicle activity, and more

Tools & Technologies
--------------------
- Python: Data generation, cleaning, and export to CSV
- SQL: Schema creation, joins, and analytical queries
- Power BI: Dashboard design, slicers, KPI cards, and visuals
- Excel: Time formatting and preprocessing

Repository Structure
--------------------
/python_scripts
    └── EV charging dataset generation.Py
/python_generated_csv
    └── Stations.csv
    └── Vehicles.csv
    └── Sessions.csv
    └── Weather.csv

/sql_queries
    └── creation.sql
    

/csv_outputs
    └── peak_hours.csv
    └── Charging Sessions During Rain.csv
    └── most active vehicles.csv
    └── Revenue per Station.csv
    └── Sessions per Station.csv
    └── Station Utilization by Location.csv
    └── Top Manufacturers by Energy Consumption.csv
    

/powerbi_dashboard
    └── ev_charge stations Analysis.pbix

/Screenshots
    └── ev_charging_dashboard.png


   
README.txt

Key Insights
------------
- Most Active Vehicles: Identified top 10 vehicles by session count
- Station Utilization: Ranked stations by usage and revenue
- Weather Impact: Quantified sessions during rainy conditions
- Energy Consumption: Highlighted top manufacturers by kWh usage
- Peak Hours: Visualized hourly demand trends across locations



Project Results
---------------
This dashboard revealed that EV charging demand peaks between 6 PM and 9 PM, with urban station leading in usage and revenue. Tata and Hyundai vehicles consumed the most energy, while rainy weather caused an 18% drop in session volume. These insights enable smarter infrastructure planning, targeted station deployment, and improved user experience.



👤 Author

AJAY ASLEEN J
GitHub : Asleen2025

