import requests, mysql.connector, json, time, os, csv, time
import pandas as pd
from mysql.connector import Error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from dotenv import load_dotenv
from m_functions import *
from io import BytesIO, StringIO
from urllib.parse import urlparse, unquote



def bf_first_setup(conn):
	new_log_entry(conn, ("h", "Bitformance", "Adding urls to sources, remember to only run once"))
	with open ("bitformance_urls.txt", "r") as file:
		file_urls = file.read().splitlines()

	for file_url in file_urls:
		url = file_url
		decoded_url = unquote(file_url)
		decoded_url = decoded_url.replace(" ", "")
		file_name = urlparse(decoded_url).path.split("/")[-1]
		file_name = file_name.replace("%20","")
		file_name = file_name.replace(",","")
		file_name = file_name.replace("&","")
		file_name = file_name.replace("(","")
		file_name = file_name.replace(")","")
		file_name = file_name.split(".")[0]
		file_name = file_name.lower()
		response = requests.get(url)
		if response.status_code == 200:
			if url.endswith(".csv"):
				df = pd.read_csv(BytesIO(response.content))
			elif url.endswith(".xlsx"):
				df = pd.read_excel(BytesIO(response.content))
		
		with conn.cursor() as cursor:
			cursor.execute("INSERT INTO bf_sources (url, sheet_name) VALUES (%s,%s)", (url, file_name))
			conn.commit()



def bf_process_url(conn, url, sheet_name):
	try:
		print(f"Connecting to url for sheet: {sheet_name}")
		response = requests.get(url, timeout=15)

		if response.status_code == 200:
			print(f"Connecting OK for sheet: {sheet_name}")
			csv_reader = csv.DictReader(StringIO(response.text))
			bf_rows = []

			for row in csv_reader:
				datestamp = datetime.strptime(row['date'], "%Y-%m-%d").date()
				total_value = row['total_value']
				total_market_cap = row['total_marketcap']

				bf_rows.append((datestamp, sheet_name, total_value, total_market_cap))

			return sheet_name, bf_rows
		
		else:
			print(f"Could not connect to url: {url}")
			new_log_entry(conn, ("h", "Bitformance", f"Could not connect to url: {url}"))
			return sheet_name, None
	
	except Exception as e:
		print(f"Bitformance error: {e}")
		new_log_entry(conn, ("h", "Bitformance", f"Bitformance error: {e}"))
		return sheet_name, None



def bf_update_data(conn):
	try:
		with conn.cursor() as cursor:
			cursor.execute("SELECT url, sheet_name FROM bf_sources ORDER BY sheet_name ASC")
			sources = cursor.fetchall()

		with ThreadPoolExecutor(max_workers=5) as executor:
			# Storing futures
			future_to_sheet = {}

			# submit tasks to the executor to store in dict
			for url, sheet_name in sources:
				time.sleep(0.1) # Sleeping because aws rate limiting :(
				future = executor.submit(bf_process_url, conn, url, sheet_name)
				future_to_sheet[future] = sheet_name
			
			# Handle finished futures
			for future in as_completed(future_to_sheet):
				sheet_name = future_to_sheet[future]
				try:
					sheet_result, bf_rows = future.result()
					if bf_rows:
						with conn.cursor() as cursor:
							cursor.executemany("""INSERT INTO
							bf_data (datestamp, sheet_name, total_value, total_marketcap)
							VALUES (%s, %s, %s, %s) 
							ON DUPLICATE KEY UPDATE 
							total_value = VALUES(total_value), 
							total_marketcap = VALUES(total_marketcap)""", 
							bf_rows)
				except Exception as e:
					print(f"Error processing sheet: {sheet_name}, {e}")

		conn.commit()
	except Error as e:
		print(f"BF Error: {e}")
		new_log_entry(conn, ("h", "Bitformance", f"Error: {e}"))
