import requests, mysql.connector, json, time, os, io, urllib.parse, sqlalchemy, pymysql
from playwright.sync_api import sync_playwright
from mysql.connector import Error
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
from m_functions import *
import pandas as pd
load_dotenv()
db_name = os.getenv("DB_NAME")
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("LOCAL_DB_PASSWORD")


# get all chain names
def dl_get_all_chain_names(conn):
	try:
		with conn.cursor() as cursor:
			cursor.execute("SELECT distinct chain FROM dl_dapp_fees_raw ORDER BY chain ASC")
			rows = cursor.fetchall()
			existing_project_ids = [row[0] for row in rows]
			return existing_project_ids
	except Error as e:
		print("sr, error getting local db projects")


# Get project slug from local db
def dl_get_existing_project_ids(conn):
	try:
		with conn.cursor() as cursor:
			cursor.execute("SELECT dapp_name FROM dl_selected_dapps ORDER BY dapp_name ASC")
			rows = cursor.fetchall()
			existing_project_ids = [row[0] for row in rows]
			return existing_project_ids
	except Error as e:
		print("dl - error getting local db projects")


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



# Update dapp list with fees > X per month
def dl_update_project_list(conn):
	try:
		with conn.cursor() as cursor:
			response = requests.get("https://api.llama.fi/overview/fees?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyFees")
			data = response.json()
			data = data["protocols"]

			value_to_check = 1
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
			cursor.execute('TRUNCATE dl_dapp_fees_raw')
			conn.commit()
			project_list = dl_get_existing_project_ids(conn)
			print(f"projects: {len(project_list)}")
			updated_project_list = []
			for project in project_list:
				if project in mapping:
					updated_project_list.append(mapping[project])
				else:
					updated_project_list.append(project)
			
			for project in updated_project_list:
				last_update = dl_check_if_data_is_up_to_date(conn, project)

				if last_update:
					print(f"dl - {project} is up to date, skipping")
				else:
					response = requests.get(f"https://api.llama.fi/summary/fees/{project}?dataType=dailyFees")
					if response.status_code == 200:
						raw_data = response.json()
						if "totalDataChartBreakdown" in raw_data:
							data = raw_data["totalDataChartBreakdown"]

							normalized_rows = []

							for entry in data:
								datestamp = datetime.utcfromtimestamp(entry[0]).strftime('%Y-%m-%d')
								chain_data = entry[1]

								for chain, projects in chain_data.items():
									total_fees = sum(projects.values())
									normalized_rows.append({
										'datestamp': datestamp,
										'project_id': project, 
										'chain': chain,
										'fees': total_fees
									})
						
						if normalized_rows:
							df = pd.DataFrame(normalized_rows)
							df = df.fillna(0)

							udb(conn, "update", "dl_dapp_fees_raw", 2, df)

					elif response.status_code == 404:
						print(f"dl - couldn't find data for: {project}")
					else:
						print(f"dl - error connecting to dl: {response.status_code} for {project}")
		print("Finished!")
	except Error as e:
		print(f"error updating defi llama data: {e}")


def dl_calculations(conn):
	cursor = conn.cursor(pymysql.cursors.DictCursor)

	cursor.execute('TRUNCATE dl_calc')
	conn.commit()

	all_chains = dl_get_all_chain_names(conn)

	calculations = []

	for chain in all_chains:
		print(f"working on {chain}")
		cursor.execute(f"SELECT datestamp, project_id, chain, fees FROM `dl_dapp_fees_raw` where chain ='{chain}' AND datestamp != NOW() ORDER BY `dl_dapp_fees_raw`.`datestamp` ASC")

		chain_data = cursor.fetchall()
		if chain_data:
			df = pd.DataFrame(chain_data, columns=['datestamp', 'project_id', 'chain', 'fees'])
		else:
			print(f"No data found for chain: {chain}")
			continue
			
		df['datestamp'] = pd.to_datetime(df['datestamp'])
		
		# Get unique dates
		unique_dates = sorted(df['datestamp'].unique())
		
		for date in unique_dates:
			# Get data for this date
			date_df = df[df['datestamp'] == date]
			
			# Calculate fees_over_0 and count_over_0 for this date
			fees_over_0 = date_df['fees'].sum()
			count_over_0 = len(date_df['project_id'].unique())
			
			# Initialize fees_over_1000 and count_over_1000
			fees_over_1000 = 0
			count_over_1000 = 0
			
			# Check each project's 30-day rolling fees
			for project_id in date_df['project_id'].unique():
				# Get project's data up to and including this date
				project_df = df[(df['project_id'] == project_id) & (df['datestamp'] <= date)]
				
				# Get the last 30 days of data (or less if not enough history)
				cutoff_date = date - pd.Timedelta(days=30)
				last_30_days_df = project_df[project_df['datestamp'] > cutoff_date]
				last_30_days_fees = last_30_days_df['fees'].sum()
				
				# Check if this project contributed >1000 fees in the last 30 days
				if last_30_days_fees > 1000:
					# Get fees for this project on this specific date
					project_fees_today = date_df[date_df['project_id'] == project_id]['fees'].sum()
					fees_over_1000 += project_fees_today
					count_over_1000 += 1
			
			# Add row to calculations
			calculations.append({
				'datestamp': date.strftime('%Y-%m-%d'),
				'chain': chain,
				'fees_over_1000': fees_over_1000,
				'count_over_1000': count_over_1000,
				'fees_over_0': fees_over_0,
				'count_over_0': count_over_0
			})

	# Create DataFrame from calculations
	if calculations:
		calc_df = pd.DataFrame(calculations)
		udb(conn, 'update', 'dl_calcs', 2, calc_df)

	else:
		print("No calculations were generated.")



def dl_update_overview_yield(engine):
	print("Updating DL yield overview")
	with sync_playwright() as p:
		browser = p.chromium.launch(headless=True)
		page = browser.new_page()

		page.set_extra_http_headers({
			"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
			"Accept-Language": "en-US,en;q=0.9",
			"Referer": "https://defillama.com/",
			"Accept-Encoding": "gzip, deflate, br"
		})

		response = page.goto("https://defillama.com/yields/overview")

		if response.status != 200:
			print(f"Page didn't load :/")
			print(f"respoinse code: {response.status}")
			browser.close()
		else:
			print("response code OK")

			try:
				page.wait_for_selector("button:has-text('Download .csv')", timeout=20000)
				print("page loaded and download button visible")
			except:
				print("no download button")
				browser.close()

			with page.expect_download() as csv_download:
				page.click("button:has-text('Download .csv')")
			
			raw_data = csv_download.value.url
			csv_data = urllib.parse.unquote(raw_data.split(",")[1])

			df = pd.read_csv(io.StringIO(csv_data))
			df = df[["timestamp", "medianAPY"]]

			df = df.rename(columns={"timestamp": "datestamp", "medianAPY": "yield_raw"})

			df["yield_7d_ma"] = df["yield_raw"].rolling(window=7).mean()
			df["yield_14d_ma"] = df["yield_raw"].rolling(window=14).mean()
			df["yield_30d_ma"] = df["yield_raw"].rolling(window=30).mean()

			df = df.where(pd.notnull(df), None)
			df.to_sql("dl_yield", con=engine, if_exists="replace", index=False)

			browser.close()


# debugging
# def dl_update_defi_llama_tables(conn):
# 	print("DL - Updating projects list and raw data")
# 	dl_update_project_raw_data(conn)

# 	print("DL - Updating calc table")
# 	dl_calculations(conn)



# db_name = os.getenv("DB_NAME")
# db_username = os.getenv("DB_USERNAME")
# db_password = os.getenv("LOCAL_DB_PASSWORD")
# conn = mysql.connector.connect(host="localhost", database=db_name, user=db_username, password=db_password, port=3303)

# dl_update_defi_llama_tables(conn)