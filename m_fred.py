import requests, mysql.connector, json, time, os
from mysql.connector import Error
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
from m_functions import *
load_dotenv()
fed_api = os.getenv("FRED_API")


# Gets a list of endpoints and the columns to save it under
def fred_get_all_projects(conn):
	with conn.cursor() as cursor:
		cursor.execute("SELECT  fred_id, db_id FROM us_fred_sources")
		result = cursor.fetchall()
		return result

# Updates the data
def fred_update_data(conn):
	with conn.cursor() as cursor:
		all_projects = fred_get_all_projects(conn)

		for fred_id, db_id in all_projects:
			print(f"Getting data for {fred_id}")
			date_value_pairs = []
			response = requests.get(f"https://api.stlouisfed.org/fred/series/observations?series_id={fred_id}&api_key={fed_api}&file_type=json")
			data = response.json()

			if "observations" in data:
				for data_point in data['observations']:
					if data_point['value'] == "." or data_point['value'].strip == "":
						data_point['value'] = None
					date_value_pairs.append((data_point['date'], data_point['value']))
				cursor.executemany(f"INSERT INTO us_fred_data (datestamp, {db_id}) VALUES (%s, %s) ON DUPLICATE KEY UPDATE {db_id} = VALUES ({db_id})", date_value_pairs)
		conn.commit()

# Todo: update this to not always go back to the beginning of time