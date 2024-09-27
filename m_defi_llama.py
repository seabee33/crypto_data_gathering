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
			cursor.execute("SELECT dapp_name FROM dl_selected_dapps ORDER BY dapp_name ASC")
			rows = cursor.fetchall()
			existing_project_ids = [row[0] for row in rows]
			return existing_project_ids
	except Error as e:
		print("sr, error getting local db projects")


# Checks if latest datestamp is 2 days ago
def dl_check_if_data_is_up_to_date(conn, project):
	try:
		with conn.cursor() as cursor:
			cursor.execute(f"SELECT datestamp FROM dl_dapp_fees_raw WHERE project_id='{project}' ORDER BY datestamp DESC LIMIT 1")
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


# Update chain list
def dl_update_chain_list(conn):
	try:
		with conn.cursor() as cursor:
			cursor.execute("SELECT chain_name FROM dl_all_chains")
			rows = cursor.fetchall()
			local_chain_list = [row[0] for row in rows]

		response = requests.get("https://api.llama.fi/overview/fees?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyFees")
		# response = requests.get("https://files.conorbriggs.com.au/dl_dailyfees.json")
		if response.status_code == 200:
			data = response.json()
			api_chain_list = data["allChains"]

			for chain in api_chain_list:
				if chain not in local_chain_list:
					pass
					# cursor.execute("INSERT INTO dl_all_chains (chain_name) VALUES (%s)", )
					# todo: finish this block of code
			
	except Error as e:
		print(e)


# Update dapp list with fees > 3000 per month
def dl_update_project_list(conn):
	try:
		with conn.cursor() as cursor:
			response = requests.get("https://api.llama.fi/overview/fees?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyFees")
			data = response.json()
			data = data["protocols"]

			value_to_check = 1000
			project_list = []
			excluded_categories = ["Chain", "Rollup"]
			excluded_projects = ["tether"]

			for item in data:
				total1m = item.get("total30d", 0)
				category = item.get("category")
				project_name = item.get("")

				if "parentProtocol" in item:
					name = item["parentProtocol"].replace("parent#", "")
				else:
					name = item["slug"]
				
				if total1m > value_to_check and category not in excluded_categories and name not in excluded_projects:
					# projects_and_value.append(f"{name}: {total1m}") #adds with fee data
					if name not in project_list:
						project_list.append(name) #adds just name
			
			cursor.executemany("INSERT IGNORE INTO dl_selected_dapps (dapp_name) VALUES(%s)", [(name,) for name in project_list])
			conn.commit()
	
	except Error as e:
		print(e)


def dl_get_mapped_name(project_id):
	mapping = {
		"apeswap":"apeswap-amm",
		"balanceddao":"balanced-exchange",
		"dopex":"stryke-clamm",
		"Edge":"blitz-perps",
		"friend-tech":"friend.tech-v1",
		"ether-fi":"ether.fi-liquid",
		"kamino-finance":"kamino-lend",
		"marinade-finance":"marinade-liquid-staking",
		"origin-defi":"origin-dollar",
		"prisma":"prismalst",
		"thala-labs":"thala-cdp",
		"venus-finance":"venus-core-pool"
	}
	if project_id in mapping:
		return mapping[project_id]
	else:
		return project_id


# Update defi llama raw fee data
def dl_update_project_raw_data(conn):

	# Update dapp list first
	dl_update_project_list(conn)

	mapping = {
		"apeswap":"apeswap-amm",
		"balanceddao":"balanced-exchange",
		"dopex":"stryke-clamm",
		"Edge":"blitz-perps",
		"friend-tech":"friend.tech-v1",
		"ether-fi":"ether.fi-liquid",
		"kamino-finance":"kamino-lend",
		"marinade-finance":"marinade-liquid-staking",
		"origin-defi":"origin-dollar",
		"prisma":"prismalst",
		"thala-labs":"thala-cdp",
		"venus-finance":"venus-core-pool"
	}

	try:
		with conn.cursor() as cursor:
			project_list = dl_get_existing_project_ids(conn)
			updated_project_list = []
			for project in project_list:
				if project in mapping:
					updated_project_list.append(mapping[project])
				else:
					updated_project_list.append(project)
			
			for project in updated_project_list:
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
								cursor.execute(f"INSERT IGNORE INTO dl_dapp_fees_raw ({column_str}) VALUES ({placeholders})", values)
						conn.commit()
						print(f"DL - updated data for {project}")
					elif response.status_code == 404:
						print(f"dl - couldn't find data for: {project}")
					else:
						print(f"dl - error connecting to dl: {response.status_code} for {project}")
		print("Finished!")
	except Error as e:
		print(f"error updating defi llama data: {e}")



def dl_setup_dapp_calc(conn):
	try:
		with conn.cursor() as cursor:
			cursor.execute("INSERT INTO dl_dapp_calc (datestamp) SELECT DISTINCT datestamp FROM dl_dapp_fees_raw ORDER BY DATESTAMP DESC;")
			conn.commit()
	except Error as e:
		print(e)


# This part is incredibily inefficient and needs to be fixed
def dl_calculate_dapps_greater_then_x(conn):
	monthly_filter_value = 1000
	try:
		with conn.cursor() as cursor:

			print("wiping calc table")
			cursor.execute("TRUNCATE dl_dapp_calc")
			print("inserting dates")
			dl_setup_dapp_calc(conn)

			cursor.execute("SELECT DISTINCT project_id FROM dl_dapp_fees_raw ORDER BY project_id ASC")
			dapps_raw = cursor.fetchall()
			dapp_list = [dl_get_mapped_name(row[0]) for row in dapps_raw]

			cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'dl_dapp_fees_raw'")
			col_names = cursor.fetchall()
			col_names = [col[0] for col in col_names]

			unwanted_cols = ["id", "project_id", "datestamp"]
			chains = [col for col in col_names if col not in unwanted_cols]

			for dapp in dapp_list:
				cursor.execute(f"SELECT MIN(datestamp) FROM dl_dapp_fees_raw WHERE project_id='{dapp}'")
				earliest_datestamp = cursor.fetchone()[0]
				# print(f"min datestamp for {dapp} is {earliest_datestamp} ")

				thirty_days_ago = (datetime.today() - timedelta(days=32)).date()
				two_days_ago = (datetime.today() - timedelta(days=2)).date()

				# print(f"30 days ago: {thirty_days_ago}, today: {two_days_ago}")
			
				for chain in chains:
					temp_thirty_days = thirty_days_ago
					temp_two_days = two_days_ago

					while temp_thirty_days > earliest_datestamp:
						# print(f"Executing1: SELECT sum({chain}) from dl_dapp_fees_raw WHERE project_id='{dapp}' AND datestamp BETWEEN '{temp_thirty_days}' AND '{temp_two_days}'")
						cursor.execute(f"SELECT sum({chain}) from dl_dapp_fees_raw WHERE project_id='{dapp}' AND datestamp BETWEEN '{temp_thirty_days}' AND '{temp_two_days}'")
						monthly_total = cursor.fetchone()[0]

						if monthly_total and monthly_total > monthly_filter_value:
							# print(f"Executing2: SELECT {chain} FROM dl_dapp_fees_raw WHERE datestamp = '{temp_two_days}' AND project_id = '{dapp}' ORDER BY datestamp DESC")
							cursor.execute(f"SELECT {chain} FROM dl_dapp_fees_raw WHERE datestamp = '{temp_two_days}' AND project_id = '{dapp}' ORDER BY datestamp DESC")
							data = cursor.fetchone()
							data_value = data[0] if data is not None else None
							count = 1 if data is not None else 0
							# print(f"Data for {dapp} on {temp_two_days} for {chain}: {data_value}")

							#update deez nuts
							update_fee_query = f"UPDATE dl_dapp_calc SET {chain} = COALESCE({chain}, 0) + COALESCE(%s,0) WHERE datestamp=%s"
							update_dapp_query = f"UPDATE dl_dapp_calc SET {chain}_c = COALESCE({chain}_c, 0) + %s WHERE datestamp=%s"
							# print(f"Executing3: {update_query}")
							cursor.execute(update_fee_query, (data_value, temp_two_days))
							cursor.execute(update_dapp_query, (count, temp_two_days))

						temp_thirty_days -= timedelta(days=1)
						temp_two_days -= timedelta(days=1)
				print(f"Updated data for {dapp}")
				conn.commit()

	except Error as e:
		print(e)



def dl_update_defi_llama_tables(conn):
	print("Updating projects list and raw data")
	# dl_update_project_raw_data(conn)

	print("Updating calc table")
	dl_calculate_dapps_greater_then_x(conn)
