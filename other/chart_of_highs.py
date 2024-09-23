#Makes a chart of the dates from highs
# check pngs in this folder

import mysql.connector
import os
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from datetime import datetime, timedelta
from collections import Counter
import pandas as pd

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
    cursor = conn.cursor()

    # Step 1: Get all project names from j_raw table
    cursor.execute("SELECT DISTINCT project_name FROM j_raw WHERE datestamp = '2024-01-01' AND price IS NOT NULL ORDER BY project_name ASC")
    projects = cursor.fetchall()
    projects = [row[0] for row in projects]
    
    # Calculate the date 2 years ago
    two_y_ago = (datetime.today() - timedelta(days=730)).date()
    
    new_high_dates = []
    old_high_dates = []

    # Step 2: For each project, get the date of the highest price from j_raw
    for project in projects:
        cursor.execute(
            """
            SELECT datestamp, price 
            FROM j_raw 
            WHERE project_name = %s 
            ORDER BY price DESC LIMIT 1
            """, (project,)
        )
        result = cursor.fetchone()
        if result:
            datestamp, highest_price = result
            if datestamp >= two_y_ago:
                new_high_dates.append(datestamp)
            else:
                old_high_dates.append(datestamp)
    
    # Step 3: Plotting the bar graphs
    
    # Get the min and max dates
    all_dates = new_high_dates + old_high_dates
    min_date = min(all_dates)
    max_date = max(all_dates)

    # Create a date range from min_date to max_date
    date_range = pd.date_range(start=min_date, end=max_date)

    # Count occurrences of each date in new_high_dates and old_high_dates
    new_high_count = Counter(new_high_dates)
    old_high_count = Counter(old_high_dates)

    # Create lists for bar chart values
    new_high_values = [new_high_count.get(date.date(), 0) for date in date_range]
    old_high_values = [old_high_count.get(date.date(), 0) for date in date_range]

    # Plot new highs bar graph
    plt.figure(figsize=(10, 5))
    plt.bar(date_range, new_high_values, color='green')
    plt.title("Projects with All-Time Highs in the Last 2 Years")
    plt.xlabel("Date")
    plt.ylabel("Number of Projects")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("new_highs.png")
    plt.show()

    # Plot old highs bar graph
    plt.figure(figsize=(10, 5))
    plt.bar(date_range, old_high_values, color='red')
    plt.title("Projects with All-Time Highs Older than 2 Years")
    plt.xlabel("Date")
    plt.ylabel("Number of Projects")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("old_highs.png")
    plt.show()

finally:
    cursor.close()
    conn.close()
    print("Done")
