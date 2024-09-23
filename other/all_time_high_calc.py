#This script generates a list of all the projects that have had an all time high in the last 2 years and all that haven't
# Check all_time_high_data.txt to see data

import mysql.connector
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

# Database connection details
db_name = os.getenv("DB_NAME")
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("LOCAL_DB_PASSWORD")

# Connect to the database
conn = mysql.connector.connect(
    host="localhost",
    database=db_name,
    user=db_username,
    password=db_password
)


try:
    with conn.cursor() as cursor:
        # Step 1: Get all project names from all_projects table
        cursor.execute("SELECT project_name FROM j_raw WHERE datestamp='2024-01-01' and price IS NOT NULL ORDER BY project_name ASC")
        projects = cursor.fetchall()
        projects = [row[0] for row in projects]
        
        two_y_ago = (datetime.today() - timedelta(days=730)).date()
        
        new_high = []
        old_high = []

        # Step 2: For each project, get the date of the highest price from j_raw
        for project in projects:
            cursor.execute(
                f"""
                SELECT datestamp, price FROM j_raw where project_name = '{project}' ORDER BY price DESC LIMIT 1
                """
            )
            result = cursor.fetchone()
            if result:
                datestamp, highest_price = result
                if datestamp >= two_y_ago:
                    new_high.append(f"{project} - ATH: {datestamp} - {highest_price}")
                else:
                    old_high.append(f"{project} - ATH: {datestamp} - {highest_price}")
        
        with open("highs.txt", "w") as f:
            f.write(f"New Highs: {len(new_high)} projects \n")
            for a in new_high:
                f.write(f"{a}\n")
            f.write(f"\n\nOld High: {len(old_high)} projects\n")
            for a in old_high:
                f.write(f"{a}\n")

finally:
    conn.close()
    print("Done")
