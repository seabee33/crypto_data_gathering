import requests, mysql.connector, json, time, os
from mysql.connector import Error
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
from m_functions import *
load_dotenv()
db_name = os.getenv("DB_NAME")
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("LOCAL_DB_PASSWORD")
conn = mysql.connector.connect(host="localhost", database=db_name, user=db_username, password=db_password)


# Get project slug from local db
def dl_get_existing_project_ids(conn):
	try:
		with conn.cursor() as cursor:
			cursor.execute("SELECT project_name FROM dl_selected_projects")
			rows = cursor.fetchall()
			existing_project_ids = [row[0] for row in rows]
			return existing_project_ids
	except Error as e:
		print("sr, error getting local db projects")

def dl_check_if_data_is_up_to_date(conn, project):
	try:
		with conn.cursor() as cursor:
			cursor.execute(f"SELECT datestamp FROM dl_protocol_fees_raw WHERE project_id='{project}' ORDER BY datestamp DESC LIMIT 1")
			raw_data = cursor.fetchone()

			if raw_data is None or raw_data[0] is None:
				return False
				# no data, needs update
			else:
				day_m2 = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")
				if raw_data[0].strftime("%Y-%m-%d") == day_m2:
					return True
					# last update date is 2 days ago
				else:
					return False
	except Error as e:
		print("dl - error getting project update date")

def dl_update_project_raw_data(conn):
	try:
		with conn.cursor() as cursor:
			project_list = dl_get_existing_project_ids(conn)
			for project in project_list:
				last_update = dl_check_if_data_is_up_to_date(conn, project)
				if last_update:
					print(f"{project} is up to date, skipping")
				else:
					response = requests.get(f"https://api.llama.fi/summary/fees/{project}?dataType=dailyFees")
					if response.status_code == 200:
						data = response.json()
						data = data["totalDataChartBreakdown"]

						for entry in data:
							datestamp = datetime.utcfromtimestamp(entry[0]).strftime('%Y-%m-%d')
							chain_data = entry[1]

							summed_data = {}
							columns = ["project_id", "datestamp"]
							values = [project, datestamp]

							if hasattr(chain_data, "items"):
								for chain, projects in chain_data.items():
									total = sum(projects.values())
									columns.append(chain)
									values.append(total)
							
							column_str = ", ".join(columns)
							placeholders = ", ".join(["%s"] * len(values))

							excluded_dates = []
							excluded_dates.append(datetime.today().strftime("%Y-%m-%d"))
							excluded_dates.append((datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d"))

							if values[1] not in excluded_dates:
								cursor.execute(f"INSERT IGNORE INTO dl_protocol_fees_raw ({column_str}) VALUES ({placeholders})", values)
						conn.commit()
						print(f"DL - updated data for {project}")
					elif response.status_code == 404:
						print(f"dl - couldn't find data for: {project}")
					else:
						print(f"dl - error connecting to dl: {response.status_code} for {project}")
	except Error as e:
		print(f"error updating defi llama data: {e}")

dl_update_project_raw_data(conn)
