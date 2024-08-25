import requests, mysql.connector, json, time, os, sys, concurrent.futures
from mysql.connector import Error
from datetime import datetime
from datetime import timedelta
from dotenv import load_dotenv
from m_functions import *
load_dotenv()
tt_api_key = os.getenv("TT_API_KEY")
db_password = os.getenv("LOCAL_DB_PASSWORD")
DEBUGGING = os.getenv("DEBUGGING")

def tt_api():
    return os.getenv("TT_API_KEY")

# Save available metrics from API and compare agains db columns (TOKEN TERMINAL)
def tt_compare_available_metrics(conn):
	tt_api_key = tt_api()
	headers = {"accept": "application/json", "authorization": f"Bearer {tt_api_key}"}
	current_date = datetime.now().strftime("%Y-%m-%d")
	unique_items = []

	try:
		with conn.cursor() as cursor:
			all_projects_list = tt_get_existing_project_ids(cursor)
			
			if all_projects_list == []:
				print("Project list empty")
				new_log_entry(conn, ("h", "Token Terminal", "No projects in projects list, can not continue"))

			for project_id in all_projects_list:
				print(f"Checking available metrics for {project_id}")
				new_log_entry(conn, ("g", "token terminal", f"Checking available metrics for {project_id}"))
				response = requests.get(f"https://api.tokenterminal.com/v2/projects/{project_id}", headers=headers)
				data = response.json()
				data = list(data["data"]["metric_availability"].keys())

				for metric in data:
					cursor.execute("SELECT project_metric from tt_available_project_metrics")
					all_metrics_from_db = cursor.fetchall()

					if all_metrics_from_db == []:
						current_metrics_list = []
					else:
						current_metrics_list = [metric[0] for metric in all_metrics_from_db]
					
					if metric not in current_metrics_list:
						print(f"'{metric} not found, adding to DB'" if DEBUGGING else None)
						new_log_entry(conn, ("m", "token terminal", f"'{metric} not found, adding to DB'"))
						query = f"INSERT INTO tt_available_project_metrics (project_metric, date_added) VALUES (%s, %s)"
						cursor.execute(query, (metric, current_date))
					conn.commit()
			db_cols = tt_check_cols_in_db(conn)
			available_metrics_from_db = tt_get_all_available_metrics_from_db(conn)

			combined_columns = db_cols + available_metrics_from_db
			unique_items = [item for item in combined_columns if combined_columns.count(item) == 1]
			return unique_items
					
	except Error as e:
		print(e)


def tt_get_all_available_metrics_from_db(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT project_metric FROM tt_available_project_metrics")
            rows = cursor.fetchall()
            unique_metrics = [row[0] for row in rows]
            return unique_metrics
    except Error as e:
        print("Error getting db metrics")



# Returns a list of all column names in the metrics table (TOKEN TERMINAL)
def tt_check_cols_in_db(conn):
	to_be_removed = ['id', 'datestamp', 'project_name', 'project_id']
	try:
		with conn.cursor() as cursor:
			cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='tt_all_metrics_data'")
			raw_data = cursor.fetchall()
			column_names = [column[0] for column in raw_data]
			column_names = list(set(column_names))

			for item_name in to_be_removed:
				if item_name in column_names:
					column_names.remove(item_name)
			return column_names
	except Error as e:
		print("Error: ", e)
		new_log_entry(conn, ("h", "Token Terminal", "Can't get column names for metrics table"))
		



# return a list of all market secords in db (TOKEN TERMINAL)
def tt_get_existing_market_sector_ids(cursor):
	cursor.execute("SELECT market_sector_id FROM tt_available_market_sectors")
	rows = cursor.fetchall()
	existing_market_sector_ids = [row[0] for row in rows]
	return existing_market_sector_ids


# return a list of all projects in db (TOKEN TERMINAL)
def tt_get_existing_project_ids(cursor):
	cursor.execute("SELECT project_id FROM tt_all_projects")
	rows = cursor.fetchall()
	existing_project_ids = [row[0] for row in rows]
	return existing_project_ids


# Checks most recent datestamp in db for project (TOKEN TERMINAL)
def tt_get_most_recent_update(cursor, project_id):
	cursor.execute(f"SELECT datestamp FROM tt_all_metrics_data WHERE project_id='{project_id}' ORDER BY datestamp DESC LIMIT 1")
	data = cursor.fetchone()
	if data == None:
		return None
	else:
		datestamp = data[0]
		new_datestamp = datestamp + timedelta(days=1)
		return new_datestamp.strftime("%Y-%m-%d")





# Gets current data from api after checking last datestamp (TOKEN TERMINAL)
def tt_update_project_metrics(cursor, conn, tt_api_key):
	print("Updating project metrics")
	new_log_entry(conn, ("g", "token terminal", "Updating project metrics"))
	
	all_project_ids = tt_get_existing_project_ids(cursor)

	for project_id in all_project_ids:

		last_update = tt_get_most_recent_update(cursor, project_id)

		if last_update != None:
			url = f"https://api.tokenterminal.com/v2/projects/{project_id}/metrics?start={last_update}"
			headers = {"accept": "application/json", "authorization": f"Bearer {tt_api_key}"}
			response = requests.get(url, headers=headers)

			if response.status_code == 200:
				data = response.json()
				data = data.get("data", [])

				if data != []:
					for item in data:
						original_datestamp = datetime.strptime(item['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
						formatted_datestamp = original_datestamp.strftime("%Y-%m-%d")
						item['timestamp'] = formatted_datestamp
						columns = ', '.join(item.keys())
						columns = columns.replace("timestamp", "datestamp")
						values = ', '.join(['%s'] * len(item))

						query = f"INSERT INTO tt_all_metrics_data ({columns}) VALUES ({values})"
						cursor.execute(query, list(item.values()))
						print(f"{project_id} data updated to {formatted_datestamp}")
						new_log_entry(conn, ("g", "token terminal", f"{project_id} data updated to {formatted_datestamp}"))

					conn.commit()
				else:
					print(f"No new update for {project_id}")
					new_log_entry(conn, ("g", "token terminal", f"No new update for {project_id}"))

			else:
				print(f"Token Terminal API response code: {response.status_code} for {project_id}")
				new_log_entry(conn, ("h", "token terminal", f"API response code: {response.status_code}  for {project_id}"))
		else:
			# No data in table
			url = f"https://api.tokenterminal.com/v2/projects/{project_id}/metrics"
			headers = {"accept": "application/json", "authorization": f"Bearer {tt_api_key}"}
			response = requests.get(url, headers=headers)

			if response.status_code == 200:
				data = response.json()
				data = data.get("data", [])

				if data != []:
					for item in data:
						original_datestamp = datetime.strptime(item['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
						formatted_datestamp = original_datestamp.strftime("%Y-%m-%d")
						item['timestamp'] = formatted_datestamp
						columns = ', '.join(item.keys())
						columns = columns.replace("timestamp", "datestamp")
						values = ', '.join(['%s'] * len(item))

						query = f"INSERT INTO tt_all_metrics_data ({columns}) VALUES ({values})"
						cursor.execute(query, list(item.values()))
					
					conn.commit()
					print(f"{project_id} data updated (FIRST DOWNLOAD)")
					new_log_entry(conn, ("l", "token terminal", f"{project_id} data updated (FIRST DOWNLOAD)"))
				else:
					print("No data received ????????????????")
					new_log_entry(conn, ("l", "token terminal", "No data received ????????????????"))

			else:
				print(f"Token Terminal API response code: {response.status_code}")
				new_log_entry(conn, ("h", "token terminal", f"API response code: {response.status_code}"))





# Updates database with a list of all project ids (TOKEN TERMINAL)
def tt_update_project_list(cursor, conn, tt_api_key):
	new_log_entry(conn, ("g", "token terminal", "Updating project list"))
	print("Updating project list")
	url = "https://api.tokenterminal.com/v2/projects"
	headers = {"accept": "application/json", "authorization": f"Bearer {tt_api_key}"}
	response = requests.get(url, headers=headers)

	if response.status_code == 200:
		new_log_entry(conn, ("g", "token terminal", "Response code OK"))
		data = response.json()
		projects = data.get("data", [])

		# Get existing data
		existing_project_ids = tt_get_existing_project_ids(cursor)

		new_projects = []
		current_date = datetime.now().strftime("%Y-%m-%d")

		for project in projects:
			project_name = project.get("name", "")
			project_id = project.get("project_id", "")
			symbol = project.get("symbol", "")
			project_url = project.get("url", "")

			if project_id not in existing_project_ids:
				new_projects.append((project_name, project_id, symbol, project_url, current_date))
				print(f"New project: {project_name}")
			

		if new_projects:
			query = "INSERT INTO tt_all_projects (project_name, project_id, symbol, project_url, date_added) VALUES (%s, %s, %s, %s, %s)"
			cursor.executemany(query, new_projects)
			conn.commit()

			print(len(new_projects), " new projects added to DB")
			new_log_entry(conn, ("l", "token terminal", f"{len(new_projects)} new projects added"))
		else:
			print("No new projects since last update")
			new_log_entry(conn, ("g", "token terminal", "No new projects since last update"))
	else:
		print(f"Token Terminal API response code: {response.status_code}")
		new_log_entry(conn, ("g", "token terminal", "No new projects since last update"))


# Updates database with a list of all market secors
def tt_update_all_market_sectors_list(cursor, conn, tt_api_key):
	print("Updating market sector list")
	new_log_entry(conn, ("g", "token terminal", "Updating market sector list"))
	url = "https://api.tokenterminal.com/v2/market-sectors"
	headers = {"accept": "application/json", "authorization": f"Bearer {tt_api_key}"}
	response = requests.get(url, headers=headers)

	if response.status_code == 200:
		new_log_entry(conn, ("g", "token terminal", "Status code OK"))
		data = response.json()
		market_sectors = data.get("data", [])

		# Get existing data
		existing_market_sector_ids = tt_get_existing_market_sector_ids(cursor)

		new_market_sectors = []
		current_date = datetime.now().strftime("%Y-%m-%d")

		for sector in market_sectors:
			ms_id = sector.get("market_sector_id", "")
			ms_name = sector.get("name", "")
			ms_url = sector.get("url", "")

			if ms_id not in existing_market_sector_ids:
				new_market_sectors.append((ms_id, ms_name, ms_url, current_date))
		

		if new_market_sectors:
			query = "INSERT INTO tt_available_market_sectors (market_sector_id, sector_name, url, date_added) VALUES (%s, %s, %s, %s)"
			cursor.executemany(query, new_market_sectors)
			conn.commit()

			print(len(new_market_sectors), " new projects added to DB")	
			new_log_entry(conn, ("l", "token terminal", f"{len(new_market_sectors)} new market secors added to db"))
		else:
			print("No new projects since last update")
			new_log_entry(conn, ("g", "token terminal", "No new projects since last update"))
	else:
		print(f"Token Terminal API response code: {response.status_code}")
		new_log_entry(conn, ("h", "token terminal", f"Token Terminal API response code: {response.status_code}"))
		exit()


def tt_get_all_market_sector_ids(cursor):
	cursor.execute("SELECT market_sector_id FROM tt_available_market_sectors")
	rows = cursor.fetchall()
	existing_project_ids = [row[0] for row in rows]
	return existing_project_ids


# Update projects table to include market sectors
def tt_update_project_ids_with_market_sector(cursor, conn, tt_api_key):
	new_log_entry(conn, ("g", "token terminal", "Adding market secors to project ids"))
	all_market_sectors = tt_get_all_market_sector_ids(cursor)

	for market_sector in all_market_sectors:
		response = requests.get(f"https://api.tokenterminal.com/v2/market-sectors/{market_sector}", headers={"accept": "application/json", "authorization": f"Bearer {tt_api_key}"})
		data = response.json()

		projects = data.get("data", {}).get("projects",[])
		
		for project in projects:
			project_id = project['project_id']

			cursor.execute("UPDATE tt_all_projects SET market_sector=%s WHERE project_id=%s", (market_sector, project_id))

	conn.commit()
	
