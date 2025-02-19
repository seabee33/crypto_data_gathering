import requests, os, mysql.connector, json, pandas as pd, time
from mysql.connector import Error
from dotenv import load_dotenv
from datetime import timedelta, datetime
from dateutil import parser
from m_functions import *


load_dotenv()
sr_api_key = os.getenv("STAKING_REWARDS_API")
db_name = os.getenv("DB_NAME")
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("LOCAL_DB_PASSWORD")


sr_api = "https://api.stakingrewards.com/public/query"
sr_headers = {"Content-Type": "application/json", "X-API-KEY": sr_api_key}

# Get project slug from local db
def sr_get_existing_project_ids(conn):
	try:
		with conn.cursor() as cursor:
			cursor.execute("SELECT  project_id FROM sr_projects")
			rows = cursor.fetchall()
			existing_project_ids = [row[0] for row in rows]
			return existing_project_ids
	except Error as e:
		print("sr, error getting local db projects")


# Get projects from api
def sr_get_all_projects_from_api(conn):
	try:
		query1 = """
			query {
				assets(where: { slugs: ["ethereum-2-0"] }, limit: 1) {
					metrics(
						where: { metricKeys: ["active_validators"], createdAt_lt: "2021-06-28" }
						limit: 10
						order: { createdAt: desc }
						interval: day
					) {
						defaultValue
						createdAt
					}
				}
			}
		"""
		
		req = {"query":query1}
		response = requests.post(sr_api, json=req, headers=sr_headers)
		data = response.json()

		if response.status_code == 200:
			if "errors" in data:
				print("error: ", data["errors"][0]["message"])
			elif "data" in data and "assets" in data["data"]:
				project_data = []
				for project in data["data"]["assets"]:
					project_data.append((project["slug"], project["name"], project["symbol"]))
				return project_data
			else:
				print("Something broke")


		else:
			print("error: ", response.status_code)
	except Error as e:
		print("sr - error getting projects from api: ", e)


def sr_update_projects_list(conn):
	try:
		local_projects = sr_get_existing_project_ids(conn)
		api_projects = sr_get_all_projects_from_api(conn)
		new_projects = []
		
		for project in api_projects:
			if project[0] not in local_projects:
				p_slug = project[0]
				p_name = project[1]
				p_symbol = project[2]
				new_projects.append((p_slug, p_name, p_symbol))
				
		with conn.cursor() as cursor:
			cursor.executemany("INSERT IGNORE INTO sr_projects (project_id, project_name, project_symbol) VALUES (%s, %s, %s)", new_projects)
			conn.commit()
	except Error as e:
		print("sr - error updating projects: ", e)


# Gets a list of projects that we want data for
def sr_get_selected_projects(conn):
	try:
		with conn.cursor() as cursor:
			cursor.execute("SELECT  project_id FROM sr_selected_projects ORDER BY project_id ASC")
			rows = cursor.fetchall()
			wanted_project_ids = [row[0] for row in rows]
			return wanted_project_ids
	except Error as e:
		print("sr - error getting wanted projects list: ", e)


def sr_get_selected_metrics(conn):
	try:
		with conn.cursor() as cursor:
			cursor.execute("SELECT  metric_name FROM sr_selected_metrics ORDER BY metric_name ASC")
			rows = cursor.fetchall()
			wanted_project_ids = [row[0] for row in rows]
			return wanted_project_ids
	except Error as e:
		print("sr - error getting wanted metrics list: ", e)



# Gets last update date for project
def sr_get_last_update(conn, project, col):
	with conn.cursor() as cursor:
		cursor.execute(f"SELECT datestamp FROM sr_raw_data WHERE project_id='{project}' and {col} IS NOT NULL ORDER BY datestamp DESC LIMIT 1")
		raw_data = cursor.fetchone()

		if raw_data is None or raw_data[0] is None:
			return "2024-11-27"
		else:
			date_m2 = (raw_data[0] - timedelta(days=2)).strftime("%Y-%m-%d")
			return date_m2

# to do, get data for 2024
# also check to make sure the date being returned is the last edited date
# also make sure data is being filled for 2024, should be because data is being gotten in asc order


def sr_update_raw_data(conn):
	try:
		with conn.cursor() as cursor:
			selected_project_list = sr_get_selected_projects(conn)
			# selected_project_list.remove("axelar")
			# selected_project_list.remove("aptos")
			# selected_project_list.remove("avalanche")
			# selected_project_list.remove("binance-smart-chain")
			# selected_project_list.remove("bitcoin")
			# selected_project_list.remove("cardano")
			# selected_project_list.remove("cosmos")
			# selected_project_list.remove("crypto-com-coin")
			# selected_project_list.remove("elrond")
			# selected_project_list.remove("ethereum-2-0")
			# selected_project_list.remove("fantom")
			# selected_project_list.remove("flow")
			# selected_project_list.remove("fuse-network-token")
			# selected_project_list.remove("injective-protocol")
			# selected_project_list.remove("matic-network")
			# selected_project_list.remove("near-protocol")
			# selected_project_list.remove("optimism")
			# selected_project_list.remove("polkadot")
			# selected_project_list.remove("sei-network")
			# selected_project_list.remove("solana")
			# selected_project_list.remove("stacks")
			# selected_project_list.remove("sui")


			selected_metrics = sr_get_selected_metrics(conn)

			for project_name in selected_project_list:
				for metric_requested in selected_metrics:
					project_data_storage = []
					last_update_for_project_metric = sr_get_last_update(conn, project_name, metric_requested)
					current_date_minus_1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
					
					current_date_minus_1_dt = datetime.strptime(current_date_minus_1, "%Y-%m-%d")
					last_update_for_project_metric_dt = datetime.strptime(last_update_for_project_metric, "%Y-%m-%d")
					days_diff = (current_date_minus_1_dt - last_update_for_project_metric_dt).days
					if days_diff > 400:
						days_diff = 370
					if days_diff == 0:
						days_diff = 1

						# 2020 means all of 2019 data
						#change created at
			
					query = f"""
						query {{
							assets(where: {{slugs: ["{project_name}"]}}, limit: 1) {{
							metrics(
								where: {{metricKeys: ["{metric_requested}"], createdAt_lt: "2025-02-17"}}, 
								limit: 20
								order: {{createdAt: desc}}
								interval: day
							) {{
								defaultValue
								createdAt
								}}
							}}
						}}
					"""
					# print(query)
					response = requests.post(sr_api, json={"query": query}, headers=sr_headers)
					if response.status_code == 200:
						data = response.json()
						with open("data.json", "w") as f:
							json.dump(data, f, indent=4)

						if "errors" not in data:
							for asset_data in data["data"]["assets"]:
								if asset_data["metrics"] is not None:
									print(f"Data found for {project_name} - {metric_requested}")
									for metric_data in asset_data["metrics"]:
										project_data_storage.append({
											"datestamp": parser.parse(metric_data["createdAt"]).date(),
											"project_id":project_name,
											metric_requested: metric_data["defaultValue"]
										})
									df = pd.DataFrame(project_data_storage)

									dupes = df[df.duplicated(subset=["datestamp"], keep=False)]
									if not dupes.empty:
										print(f"STAKING REWARDS DUPE FOUND FOR {project_name} - {metric_requested}")
										new_log_entry(conn, ("h", "staking rewards", f"Data error! {project_name} - {metric_requested}"))
										df.to_csv(f"{project_name}-{metric_requested}.csv", index=False)

									# print(df)

									udb(conn, "update", "sr_raw_data", 2, df)
									conn.commit()
									print("updated db")
									time.sleep(1)
								else:
									print(f"No metric data for {project_name} - {metric_requested}")
									time.sleep(1)
						else:
							print(f"sr - error: {data['errors'][0]['message']}")
							new_log_entry(conn, ("h", "staking rewards", f"error: {data['errors'][0]['message']}"))
							if data['errors'][0]['message'] == 'reached monthly quota limit':
								return
					else:
						print(f"SR error - response code {response.status_code}")

	except Error as e:
		print(f"error updating sr data: {e}")



conn = mysql.connector.connect(host="localhost", database=db_name, user=db_username, password=db_password, port=3303)

sr_update_raw_data(conn)


