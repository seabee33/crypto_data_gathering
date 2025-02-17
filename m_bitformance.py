import requests, mysql.connector, json, time, os, csv, time
import pandas as pd
from mysql.connector import Error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()
from m_functions import *
from io import BytesIO, StringIO
from urllib.parse import urlparse, unquote

bf_api_public = os.getenv("BITFORMANCE_API_PUB")
bf_api_priv = os.getenv("BITFORMANCE_API_PRIV")

def bf_get_coin_info(conn):
	try:
		with conn.cursor() as cursor:
			cursor.execute("TRUNCATE `bf_coin_data`")
			page = 1
			keep_going = "y"
			while keep_going == "y":
				response = requests.get(f"https://api.bitformance.com/api/v2/get-all-coins?page={page}", headers={"API-KEY": bf_api_public,"API-SECRET-KEY": bf_api_priv})
				if response.status_code == 200:
					data = response.json()
					# Check for empty list
					if not data["data"]:
						keep_going = "n"
					else:
						df = pd.json_normalize(data["data"])
						df = df[["coin_slug", "name", "symbol", "tier_lvl_1", "tier_lvl_2"]]
						# print(data)
						# print(df)
						page += 1
						udb(conn, "update", "bf_coin_data", 1, df)
				else:
					keep_going = "n"
					print(response.status_code)
	except Error as e:
		print(f"bf - trying to update coins, error: {e}")


def bf_update_data(conn):
	print("bitformance - beginning update")
	new_log_entry(conn, ("g", "bitformance", "beginning update"))

	bf_get_coin_info(conn)

	with conn.cursor() as cursor:
		cursor.execute("TRUNCATE `bf_index_holdings`")
		conn.commit()

	# ===================== Top 200 ===================== 

	# MC
	response = requests.get(
		f"https://api.bitformance.com/api/v2/get-top200-data", 
		headers={"API-KEY": bf_api_public,"API-SECRET-KEY": bf_api_priv}, 
		params={"timeseries_interval": "daily", "weighting_method": "market_cap", 'metric':'closing_price'})
	data = response.json()
	df = pd.json_normalize(data["data"]["daily_timeseries_data"])
	df.rename(columns={"date":"datestamp", "value":f"market_cap_value"}, inplace=True)
	df.insert(1, "sheet_name", "top200")
	udb(conn, "update", "bf_raw_data", 2, df)

	# Total market cap
	response = requests.get(
		f"https://api.bitformance.com/api/v2/get-top200-data", 
		headers={"API-KEY": bf_api_public,"API-SECRET-KEY": bf_api_priv}, 
		params={"timeseries_interval": "daily", "weighting_method": "market_cap", "metric": "market_cap"})
	data = response.json()
	df = pd.json_normalize(data["data"]["daily_timeseries_data"])
	df.rename(columns={"date":"datestamp", "value":f"total_marketcap_value"}, inplace=True)
	df.insert(1, "sheet_name", "top200")
	udb(conn, "update", "bf_raw_data", 2, df)
	
	# mc constituents table
	index_holdings_df = pd.json_normalize(data['data']['top_200_rebalanced_constituents'])
	index_holdings_df = index_holdings_df.explode("constituents")
	index_holdings_df.insert(0,"index_name", "top_200_mc")
	index_holdings_df['ticker'] = index_holdings_df['constituents'].apply(lambda x: x["symbol"] if isinstance(x, dict) else x)
	index_holdings_df.drop(columns=["constituents"], inplace=True)
	udb(conn, "ignore", "bf_index_holdings", 2, index_holdings_df)


	# EQW
	response = requests.get(
		f"https://api.bitformance.com/api/v2/get-top200-data", 
		headers={"API-KEY": bf_api_public,"API-SECRET-KEY": bf_api_priv}, 
		params={"timeseries_interval": "daily", "weighting_method": "equal_weight"})
	data = response.json()
	df = pd.json_normalize(data["data"]["daily_timeseries_data"])
	df.rename(columns={"date":"datestamp", "value":f"equal_weight_value"}, inplace=True)
	df.insert(1, "sheet_name", "top200")
	udb(conn, "update", "bf_raw_data", 2, df)

	# eqw constituents table
	index_holdings_df = pd.json_normalize(data['data']['top_200_rebalanced_constituents'])
	index_holdings_df = index_holdings_df.explode("constituents")
	index_holdings_df.insert(0,"index_name", "top_200_eqw")
	index_holdings_df['ticker'] = index_holdings_df['constituents'].apply(lambda x: x["symbol"] if isinstance(x, dict) else x)
	index_holdings_df.drop(columns=["constituents"], inplace=True)
	udb(conn, "ignore", "bf_index_holdings", 2, index_holdings_df)

	# Total market cap
	response = requests.get(
		f"https://api.bitformance.com/api/v2/get-top200-data", 
		headers={"API-KEY": bf_api_public,"API-SECRET-KEY": bf_api_priv}, 
		params={"timeseries_interval": "daily", "weighting_method": "market_cap", "metric": "market_cap"})
	data = response.json()
	df = pd.json_normalize(data["data"]["daily_timeseries_data"])
	df.rename(columns={"date":"datestamp", "value":f"total_marketcap_value"}, inplace=True)
	df.insert(1, "sheet_name", "top200")
	udb(conn, "update", "bf_raw_data", 2, df)




	# ===================== Altcoin ===================== 

	# MC
	response = requests.get(
		f"https://api.bitformance.com/api/v2/get-bitformance-altcoin-index", 
		headers={"API-KEY": bf_api_public,"API-SECRET-KEY": bf_api_priv}, 
		params={"timeseries_interval": "daily", "weighting_method": "market_cap", 'metric':'closing_price'})
	data = response.json()
	df = pd.json_normalize(data["data"]["daily_timeseries_data"])
	df.rename(columns={"date":"datestamp", "value":f"market_cap_value"}, inplace=True)
	df.insert(1, "sheet_name", "top200-altcoin")
	udb(conn, "update", "bf_raw_data", 2, df)

	# Total market cap
	response = requests.get(
		f"https://api.bitformance.com/api/v2/get-bitformance-altcoin-index", 
		headers={"API-KEY": bf_api_public,"API-SECRET-KEY": bf_api_priv}, 
		params={"timeseries_interval": "daily", "weighting_method": "market_cap", "metric": "market_cap"})
	data = response.json()
	df = pd.json_normalize(data["data"]["daily_timeseries_data"])
	df.rename(columns={"date":"datestamp", "value":f"total_marketcap_value"}, inplace=True)
	df.insert(1, "sheet_name", "top200-altcoin")
	udb(conn, "update", "bf_raw_data", 2, df)
	

	# EQW
	response = requests.get(
		f"https://api.bitformance.com/api/v2/get-bitformance-altcoin-index", 
		headers={"API-KEY": bf_api_public,"API-SECRET-KEY": bf_api_priv}, 
		params={"timeseries_interval": "daily", "weighting_method": "equal_weight"})
	data = response.json()
	df = pd.json_normalize(data["data"]["daily_timeseries_data"])
	df.rename(columns={"date":"datestamp", "value":f"equal_weight_value"}, inplace=True)
	df.insert(1, "sheet_name", "top200-altcoin")
	udb(conn, "update", "bf_raw_data", 2, df)

	# Total market cap
	response = requests.get(
		f"https://api.bitformance.com/api/v2/get-bitformance-altcoin-index", 
		headers={"API-KEY": bf_api_public,"API-SECRET-KEY": bf_api_priv}, 
		params={"timeseries_interval": "daily", "weighting_method": "market_cap", "metric": "market_cap"})
	data = response.json()
	df = pd.json_normalize(data["data"]["daily_timeseries_data"])
	df.rename(columns={"date":"datestamp", "value":f"total_marketcap_value"}, inplace=True)
	df.insert(1, "sheet_name", "top200-altcoin")
	udb(conn, "update", "bf_raw_data", 2, df)


	# ===================== Update sector info ===================== 
	with conn.cursor() as cursor:
		cursor.execute("""
			UPDATE bf_index_holdings 
			join bf_coin_data 
			on bf_coin_data.symbol = bf_index_holdings.ticker
			set bf_index_holdings.coin_name = bf_coin_data.name,
			bf_index_holdings.sector = bf_coin_data.tier_lvl_1,
			bf_index_holdings.subsector = bf_coin_data.tier_lvl_2""")
		conn.commit()


	# ===================== Alt Season Indicator ===================== 
	response = requests.get(
		f"https://api.bitformance.com/api/v2/get-altcoin-season", 
		headers={"API-KEY": bf_api_public,"API-SECRET-KEY": bf_api_priv})
	data = response.json()
	df = pd.json_normalize(data["data"]["daily_timeseries_data"])
	df.rename(columns={"date":"datestamp", "value":"indicator_value"}, inplace=True)
	udb(conn, "ignore", "bf_alt_season_indicator", 1, df)







	# ===================== all indexes ===================== 

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



# db_name = os.getenv("DB_NAME")
# db_username = os.getenv("DB_USERNAME")
# db_password = os.getenv("LOCAL_DB_PASSWORD")
# conn = mysql.connector.connect(host="localhost", database=db_name, user=db_username, password=db_password, port=3303)

# bf_update_data(conn)