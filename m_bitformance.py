import requests, mysql.connector, json, time, os, csv, time
import pandas as pd
from mysql.connector import Error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from dotenv import load_dotenv
from m_functions import *
from io import BytesIO, StringIO
from urllib.parse import urlparse, unquote

def bf_update_data(conn):
	print("bitformance - beginning update")
	new_log_entry(conn, ("g", "bitformance", "beginning update"))

	bf_api_public = os.getenv("BITFORMANCE_API_PUB")
	bf_api_priv = os.getenv("BITFORMANCE_API_PRIV")
	# MC
	response = requests.get(
		f"https://api.bitformance.com/api/v2/get-top200-data", 
		headers={"API-KEY": bf_api_public,"API-SECRET-KEY": bf_api_priv}, 
		params={"timeseries_interval": "daily", "weighting_method": "marketcap"})
	data = response.json()
	df = pd.json_normalize(data["data"]["daily_timeseries_data"])
	df.rename(columns={"date":"datestamp", "value":f"market_cap_value"}, inplace=True)
	df.insert(1, "sheet_name", "top200")
	udb(conn, "update", "bf_raw_data", 2, df)


	# EW
	response = requests.get(
		f"https://api.bitformance.com/api/v2/get-top200-data", 
		headers={"API-KEY": bf_api_public,"API-SECRET-KEY": bf_api_priv}, 
		params={"timeseries_interval": "daily", "weighting_method": "equal_weight"})
	data = response.json()
	df = pd.json_normalize(data["data"]["daily_timeseries_data"])
	df.rename(columns={"date":"datestamp", "value":f"equal_weight_value"}, inplace=True)
	df.insert(1, "sheet_name", "top200")
	udb(conn, "update", "bf_raw_data", 2, df)

	# Total market cap
	response = requests.get(
		f"https://api.bitformance.com/api/v2/get-top200-data", 
		headers={"API-KEY": bf_api_public,"API-SECRET-KEY": bf_api_priv}, 
		params={"timeseries_interval": "daily", "metric": "market_cap"})
	data = response.json()
	df = pd.json_normalize(data["data"]["daily_timeseries_data"])
	df.rename(columns={"date":"datestamp", "value":f"total_marketcap_value"}, inplace=True)
	df.insert(1, "sheet_name", "top200")
	udb(conn, "update", "bf_raw_data", 2, df)


	# Get all indexes
	response = requests.get(
		f"https://api.bitformance.com/api/v2/get-sector-indexes", 
		headers={"API-KEY": bf_api_public,"API-SECRET-KEY": bf_api_priv})
	indexes = response.json()["data"]

	for index in indexes:
		index_id = index["index_info"]["_id"]
		index_weighing_method = index["index_info"]["weighting_method"]
		index_name = index["index_info"]["name"]
		if "EQW " in index_name:
			index_name = index_name.strip("EQW ")
		print(f"ID: {index_id} - Method: {index_weighing_method} - Name: {index_name}")

		# Get index data for default and eqw
		response = requests.get(
			f"https://api.bitformance.com/api/v2/get-index-data", 
			headers={"API-KEY": bf_api_public,"API-SECRET-KEY": bf_api_priv}, 
			params={"timeseries_interval": "daily", "index_id":index_id, "metric": "closing_price"})
		data = response.json()
		with open("data.json", "w") as f:
			json.dump(data, f, indent=4)
		df = pd.json_normalize(data["data"]["daily_timeseries_data"])
		df.rename(columns={"date":"datestamp", "value":f"{index_weighing_method}_value"}, inplace=True)
		df.insert(1, "sheet_name", index_name)
		udb(conn, "update", "bf_raw_data", 2, df)

		# Get index data for marketcap
		response = requests.get(
			f"https://api.bitformance.com/api/v2/get-index-data", 
			headers={"API-KEY": bf_api_public,"API-SECRET-KEY": bf_api_priv}, 
			params={"timeseries_interval": "daily", "index_id":index_id, "metric": "market_cap"})
		data = response.json()
		with open("data.json", "w") as f:
			json.dump(data, f, indent=4)
		df = pd.json_normalize(data["data"]["daily_timeseries_data"])
		df.rename(columns={"date":"datestamp", "value":f"total_marketcap_value"}, inplace=True)
		df.insert(1, "sheet_name", index_name)
		udb(conn, "update", "bf_raw_data", 2, df)
	print("bitformance - update finished")
	new_log_entry(conn, ("g", "bitformance", "finished update"))
