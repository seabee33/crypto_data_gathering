import requests, mysql.connector, json, time, os
from mysql.connector import Error
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
from m_functions import *
load_dotenv()
art_api_key = os.getenv("ART_API_KEY")
db_password = os.getenv("LOCAL_DB_PASSWORD")
api_query = {"APIKey": art_api_key}


# Gets all projects from db
def art_get_existing_projects(cursor):
	cursor.execute("SELECT artemis_id FROM art_supported_projects ORDER BY artemis_id ASC")
	rows = cursor.fetchall()
	existing_projects_list = [row[0] for row in rows]
	return existing_projects_list


def art_get_projects_in_string(cursor):
	cursor.execute("SELECT artemis_id FROM art_supported_projects ORDER BY artemis_id ASC")
	rows = cursor.fetchall()
	existing_projects_list = [row[0] for row in rows]
	return ",".join(existing_projects_list)


def art_get_selected_metrics_in_string(conn):
	with conn.cursor() as cursor:
		cursor.execute("SELECT selected_metric FROM art_selected_metrics ORDER BY selected_metric ASC")
		rows = cursor.fetchall()
		existing_projects_list = [row[0] for row in rows]
		return ",".join(existing_projects_list)


# Update all projects from api
def art_update_all_projects_list(cursor, conn, api_query):
	new_log_entry(conn, ("g", "artemis", "Checking for new projects"))
	print("Updating artemis project list")
	url = "https://api.artemisxyz.com/asset"
	headers = {"accept": "application/json"}
	response = requests.get(url, headers=headers, params=api_query)

	if response.status_code == 200:
		new_log_entry(conn, ("g", "artemis", "Response code OK"))
		data = response.json()
		projects = data.get("assets", [])

		# Get existing data
		existing_supported_projects = art_get_existing_projects(cursor)

		new_projects = []
		current_date = datetime.now().strftime("%Y-%m-%d")

		for project in projects:
			project_name = project.get("artemis_id", "")
			project_symbol = project.get("symbol", "")

			if project_name not in existing_supported_projects:
				new_log_entry(conn, ("l", "artemis", f"New project found: {project_name}"))
				new_projects.append((project_name, project_symbol, current_date))
		
		if new_projects:
			query = "INSERT INTO art_supported_projects (artemis_id, symbol, date_added) VALUES (%s, %s, %s)"
			cursor.executemany(query, new_projects)
			conn.commit()

			print(len(new_projects), " new projects added to DB")	
			new_log_entry(conn, ("l", "artemis", f"{len(new_projects)} new projects added"))

		else:
			new_log_entry(conn, ("l", "artemis", f"{len(new_projects)} new projects added"))
			print("No new artemis projects since last update")	
	else:
		new_log_entry(conn, ("h", "artemis", f"Status code '{response.statuscode} when trying to update project list'"))
		print(f"Artemis API response code: {response.status_code}")


def art_get_local_project_metrics(cursor, project_name):
	cursor.execute(f"SELECT metric_name FROM art_available_project_metrics WHERE project_name='{project_name}'")
	rows = cursor.fetchall()
	existing_project_metrics = [row[0] for row in rows]
	return existing_project_metrics


# Gets all metrics per project from artemis api
def art_get_api_project_metrics(cursor, conn, api_query):
	new_log_entry(conn, ("g", "artemis", "Checking for new metrics"))
	HEADERS = {"accept": "application/json"}
	CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")

	all_project_names = art_get_existing_projects(cursor)

	for project in all_project_names:
		response = requests.get(f"https://api.artemisxyz.com/asset/{project}/metric", headers=HEADERS, params=api_query)

		if response.status_code == 200:
			new_log_entry(conn, ("g", "artemis", "Status code OK"))
			data = response.json()
			metrics = data.get("metrics", [])
			existing_project_metrics = art_get_local_project_metrics(cursor, project)
			new_project_metric = []

			for metric in metrics:
				if metric not in existing_project_metrics:
					new_project_metric.append((project, metric, CURRENT_DATE))
			
			if new_project_metric:
				cursor.executemany("INSERT INTO art_available_project_metrics (project_name, metric_name, date_added) VALUES (%s, %s, %s)", new_project_metric)
				conn.commit()
				print(f"{len(new_project_metric)} new metrics added for project {project}")
				new_log_entry(conn, ("g", "artemis", f"{len(new_project_metric)} new metrics added for project {project}"))
			else:
				print(f"No new metrics for {project}")
				new_log_entry(conn, ("g", "artemis", "No new metrics found"))

				

		else:
			print(f"Artemis response error code: {response.status_code}")
			new_log_entry(conn, ("h", "artemis", f"Status code '{response.statuscode} when trying to update project metrics'"))



# Gets all unique metrics from db
def art_get_all_unique_metrics(cursor):
	cursor.execute(f"SELECT metric_name FROM art_unique_metrics")
	rows = cursor.fetchall()
	unique_project_metrics = [row[0] for row in rows]
	return unique_project_metrics


# Gets all metrics including duplicates for every project from db
def art_get_all_available_metrics(cursor):
	cursor.execute("SELECT metric_name FROM art_available_project_metrics")
	rows = cursor.fetchall()
	all_project_metrics = [row[0] for row in rows]
	return all_project_metrics


# Gets all unique metrics from unique_metrics table
def art_update_unique_metrics_table(cursor, conn):
	new_log_entry(conn, ("g", "artemis", "Updating unique metrics table"))
	CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
	current_unique_metrics = art_get_all_unique_metrics(cursor)
	current_all_available_metrics = art_get_all_available_metrics(cursor)

	new_unique_metrics = []

	for metric in current_all_available_metrics:
		if metric not in current_unique_metrics:
			new_unique_metrics.append((metric, CURRENT_DATE))
		

	new_unique_metrics = list(set(new_unique_metrics))

	if new_unique_metrics:
		cursor.executemany("INSERT INTO art_unique_metrics (metric_name, date_added) VALUES (%s, %s)", new_unique_metrics)
		conn.commit()

		print(f"{len(new_unique_metrics)} new unique metrics added")
		new_log_entry(conn, ("g", "artemis", f"{len(new_unique_metrics)} new unique metrics added"))
	else:
		print("No new unique metrics")
		new_log_entry(conn, ("g", "artemis", "No new unique metrics"))


def art_get_saved_ecosystem_data(cursor):
	cursor.execute(f"SELECT eco_value FROM art_supported_ecosystems")
	rows = cursor.fetchall()
	existing_supported_ecosystems = [row[0] for row in rows]
	return existing_supported_ecosystems


# Get a list of supported ecosystems (includes junk data, not main projects)
def art_update_supported_ecosystems_from_api(cursor, conn, api_query):
	CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
	HEADERS = {"accept": "application/json"}
		
	response = requests.get(f"https://api.artemisxyz.com/dev-ecosystems", headers=HEADERS, params=api_query)

	existing_ecosystems = art_get_saved_ecosystem_data(cursor)
	new_ecosystems = []

	if response.status_code == 200:
		data = response.json()

		for item in data:
			eco_label = item.get("label", "")
			eco_value = item.get("value", "")
			eco_symbol = item.get("symbol", "")

			if eco_value not in existing_ecosystems:
				new_ecosystems.append((eco_label, eco_value, eco_symbol, CURRENT_DATE))
		
		if new_ecosystems:
			cursor.executemany("INSERT INTO art_supported_ecosystems(eco_label, eco_value, eco_symbol, date_added) VALUES (%s, %s, %s, %s)", new_ecosystems)
			conn.commit()

			print(f"{len(new_ecosystems)} new ecosystems added")
		
		else:
			print("No new ecosystems since last update")

	else:
		print(response.status_code)


def art_get_wanted_metrics(cursor):
	cursor.execute("SELECT selected_metric FROM art_selected_metrics ORDER BY selected_metric ASC")
	rows = cursor.fetchall()
	existing_selected_metrics = [row[0] for row in rows]
	return existing_selected_metrics



def art_add_selected_metrics(cursor, conn):
	new_log_entry(conn, ("g", "artemis", "Updating selected metrics table to included wanted metrics"))
	wanted_metrics = ['dau', 'daily_txns', 'twitter_followers', 'price', 'mc', '24h_volume', 'fdmc', 'tvl', 'dex_volumes', 'fees', 'avg_txn_fees', 'revenue', 'dau_over_100', 'stablecoin_mc', "circulating_supply"]

	existing_selected_metrics = art_get_wanted_metrics(cursor)
	
	metrics_to_add_to_db = []

	for wanted_metric in wanted_metrics:
		if wanted_metric not in existing_selected_metrics:
			metrics_to_add_to_db.append((wanted_metric,))
	
	if metrics_to_add_to_db:
		cursor.executemany("INSERT INTO art_selected_metrics (selected_metric) VALUES (%s)", metrics_to_add_to_db)
		conn.commit()


def art_get_most_recent_metrics_data_date(conn, single_project_name):
	with conn.cursor() as cursor:
		cursor.execute("SELECT datestamp FROM art_metric_data WHERE project_name=%s ORDER BY datestamp DESC LIMIT 1", (single_project_name,))
		date = cursor.fetchone()

		if date == None:
			return None
		else:
			return date[0]



def get_date_list_from_metrics(data):
	unique_dates = set()
	for metric, metric_data in data.items():
		if isinstance(metric_data, list) and len(metric_data) > 0 and 'date' in metric_data[0]:
			for data_point in metric_data:
				unique_dates.add(data_point['date'])
	return(list(unique_dates))


def art_get_existing_metrics(conn, metrics_as_string, project_name):
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT {metrics_as_string} FROM art_metric_data WHERE project_name=%s ORDER BY datestamp DESC LIMIT 5", (project_name,))
        result = cursor.fetchall()
        return(result)


# Update metric DATA
def art_update_metric_data(cursor, conn, art_api_key):
	new_log_entry(conn, ("g", "artemis", "Updating artemis data"))
	current_date_minus_2 = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
	HEADERS = {"accept": "application/json"}

	selected_metrics = art_get_wanted_metrics(cursor)
	selected_metrics_str = art_get_selected_metrics_in_string(conn)
	project_names = art_get_existing_projects(cursor)

	for single_project_name in project_names:
		most_recent_update_date = art_get_most_recent_metrics_data_date(conn, single_project_name)

		if most_recent_update_date == None:
			is_new = True
			start_date = "2013-01-01"
			new_log_entry(conn, ("l", "artemis", f"Couldnt find date for '{single_project_name}', must be new"))
		else:
			is_new = False
			start_date = most_recent_update_date - timedelta(days=10)
			new_log_entry(conn, ("g", "artemis", f"Found date for '{single_project_name}', most recent update date {most_recent_update_date}"))
		
		custom_api_query = {"artemisIds":single_project_name,"startDate":start_date,"endDate":current_date_minus_2,"summarize":"false","APIKey":art_api_key}
		response = requests.get(f"https://api.artemisxyz.com/data/{selected_metrics_str}", headers=HEADERS, params=custom_api_query)

		if response.status_code == 200:
			new_log_entry(conn, ("g", "artemis", f"Response code OK"))
			data = response.json()

			project_data = data['data']['artemis_ids'][single_project_name]
			date_list = get_date_list_from_metrics(project_data)
			
			result_list = []  # List to hold the results
			
			for single_date in date_list:
				metric_values = []  # List to hold metric values for this date
				
				for metric in selected_metrics:
					if metric in project_data:
						metric_data = project_data[metric]
						if isinstance(metric_data, list):
							val = next((item['val'] for item in metric_data if item['date'] == single_date), None)
							metric_values.append(val if val is not None else None)
						else:
							metric_values.append(None)
					else:
						metric_values.append(None)
				
				# Combine single_date, single_project_name, and metric values
				if any(metric_values):
					result_list.append([single_date, single_project_name] + metric_values)

			column_names = ['datestamp', 'project_name'] + selected_metrics
			column_names = ['volume_24h' if _ == '24h_volume' else _ for _ in column_names]
			column_names_for_update = column_names[2:]
			column_names_str = ", ".join(column_names)
			placeholders = ", ".join(['%s'] * len(column_names))
			update_columns = ", ".join([f"{col}=VALUES({col})" for col in column_names[2:]])

			print(f"Updating data for {single_project_name}")
			cursor.executemany(f"""
			INSERT INTO art_metric_data ({column_names_str})
			VALUES ({placeholders})
			ON DUPLICATE KEY UPDATE {update_columns}
			""", result_list)
			conn.commit()
		else:
			print(f"UH OH, error code: {response.status_code}")
			new_log_entry(conn, ("h", "artemis", f"Status code '{response.statuscode} when trying to update project metrics data'"))

