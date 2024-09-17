import requests, mysql.connector, json, time, os, sys

from datetime import datetime
from mysql.connector import Error
from dotenv import load_dotenv
from m_functions import *
from collections import deque
load_dotenv()
db_name = os.getenv("DB_NAME")
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("LOCAL_DB_PASSWORD")

def calculate_sma(conn):
	# art_metric_data
	#left is name to save in table, right is column name to select from
	art_metrics = {
		"daily_trading_volume": "volume_24h",
		"avg_txn_fees": "avg_txn_fees", # not in TT
		"circulating_supply": "circulating_supply",
		"daily_txns": "daily_txns",
		"daa": "dau",
		"daa_over_100": "dau_over_100", # not in TT
		"dex_volume":"dex_volumes",  # not in TT
		"fd_marketcap":"fdmc",
		"fees":"fees",
		"marketcap":"mc",
		"price":"price",
		"revenue":"revenue",
		"stablecoin_mc":"stablecoin_mc", # not in TT
		"tvl":"tvl"
		}

	tt_metrics = {
		# tt_all_metrics_data
		"tvl":"tvl",
		"price":"price",
		"daa":"user_dau",
		"daily_txns": "transaction_count",
		"marketcap":"market_cap_circulating",
		"revenue":"revenue",
		"fees":"fees",
		"daily_trading_volume":"token_trading_volume",
		"fd_marketcap":"market_cap_fully_diluted",
		"circulating_supply":"token_supply_circulating"
		}

	try:
		with conn.cursor() as cursor:
			cursor.execute("SELECT project_id, from_table, sector FROM v_all_unique_projects WHERE sector IS NOT NULL")
			result = cursor.fetchall()

			for row in result:
				project_id,	from_table, sector = row
				all_sma_data = []

				if from_table == "art_metric_data":
					metrics = art_metrics
				elif from_table == "tt_all_metrics_data":
					metrics = tt_metrics
				else:
					print("Something went badly wrong")
					sys.exit(1)
					
				for save_as_metric, col_metric in metrics.items():
					cursor.execute(f"SELECT datestamp,{col_metric} FROM {from_table} WHERE project_name='{project_id}' ORDER BY datestamp ASC")
					data = cursor.fetchall()

					periods = [7,14,30,45,50,60,90]
					sma_values = {period: [] for period in periods}
					windows = {period: deque(maxlen=period) for period in periods}

					for date, value in data:
						if value is not None:
							for period in periods:
								windows[period].append(value)

								if len(windows[period]) == period:
									sma = sum(windows[period]) / period
									sma_values[period].append((date, sma))

					for period, values in sma_values.items():
						for date, sma in values:
							all_sma_data.append((project_id, sector, date, save_as_metric, period, sma))

				batch_size = 1000
				for size in range(0,len(all_sma_data), batch_size):
					batch = all_sma_data[size:size+batch_size]
				update_query = """
				INSERT INTO cb_metric_sma
				(project_id, sector, datestamp, metric_name, time_period, sma_value)
				VALUES (%s, %s, %s, %s, %s, %s)
				ON DUPLICATE KEY UPDATE sma_value = VALUES(sma_value)
				"""
				cursor.executemany(update_query, all_sma_data)
				conn.commit()
				print(f"Updated SMA for {project_id}")

	except Error as e:
		print("Error: ", e, f" for project: {project_id}")


# Update raw table
def calc_update_raw_table(conn):
	batch_size = 1000
	try:
		with conn.cursor() as cursor:
			# Token Terminal table
			cursor.execute("SELECT datestamp, project_name, user_dau, fees, market_cap_circulating, market_cap_fully_diluted, price, transaction_count, revenue, transaction_fee_average, user_mau, tokenholders, tvl, token_trading_volume, active_developers, active_loans, earnings, gross_profit, token_supply_circulating, token_incentives, token_supply_maximum, active_addresses_weekly FROM tt_all_metrics_data WHERE user_dau IS NOT NULL AND user_dau != '0' AND fees IS NOT NULL AND fees != '0' ORDER BY datestamp DESC")
			tt_api_table = cursor.fetchall()

			# j_raw table column names
			cols = ["datestamp", "project_name", "daa", "fees", "mc", "fdmc", "price", "transactions", "revenue", "avg_txn_fee", "maa", "tokenholders", "tvl", "volume_24h_usd", "active_developers", "active_loans", "earnings", "gross_profit", "token_supply_circulating", "token_incentives", "token_supply_maximum", "active_addresses_weekly"]
			cols_str = ", ".join(cols)
			placeholders = ", ".join(['%s'] * len(cols))
			update_cols = ", ".join([f"{col}=VALUES({col})" for col in cols[2:]])

			for i in range(0, len(tt_api_table), batch_size):
				cursor.executemany(f"INSERT INTO j_raw ({cols_str}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_cols}", tt_api_table[i:i+batch_size])
			print("Updated token terminal")

			# Artemis API table
			cursor.execute("SELECT datestamp, project_name, dau, fees, mc, fdmc, price, daily_txns, revenue, avg_txn_fees, dau_over_100, dex_volumes, tvl, stablecoin_mc, volume_24h FROM art_metric_data WHERE dau IS NOT NULL AND dau != '0' AND fees IS NOT NULL AND fees != '0' ORDER BY datestamp DESC")
			art_api_table = cursor.fetchall()

			# j_raw table column names
			cols = ["datestamp", "project_name", "daa", "fees", "mc", "fdmc", "price", "transactions", "revenue", "avg_txn_fee", "daa_over_100", "dex_volume", "tvl", "stablecoin_mc", "volume_24h_usd"]
			cols_str = ", ".join(cols)
			placeholders = ", ".join(['%s'] * len(cols))
			update_cols = ", ".join([f"{col}=VALUES({col})" for col in cols[2:]])

			for i in range(0, len(art_api_table), batch_size):
				cursor.executemany(f"INSERT INTO j_raw ({cols_str}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_cols}", art_api_table[i:i+batch_size])
			print("Updated artemis (API)")

			# Artemis SF table
			cursor.execute("SELECT datestamp, project_name, dau, fees, market_cap, fdmc, price, txns, revenue, avg_txn_fee, dau_over_100, mau, dex_volumes, tokenholder_count, tvl, stablecoin_total_supply, weekly_commits_core_ecosystem, weekly_commits_sub_ecosystem, weekly_contracts_deployed, weekly_contract_deployers, weekly_developers_core_ecosystem, weekly_developers_sub_ecosystem FROM art_sf_raw_data WHERE dau IS NOT NULL AND dau != '0' AND fees IS NOT NULL AND fees != '0' ORDER BY datestamp DESC")
			art_sf_table = cursor.fetchall()

			# j_raw table column names
			cols = ["datestamp", "project_name", "daa", "fees", "mc", "fdmc", "price", "transactions", "revenue", "avg_txn_fee", "daa_over_100", "maa", "dex_volume", "tokenholders", "tvl", "stablecoin_mc", "weekly_commits_core", "weekly_commits_sub", "weekly_contracts_deployed", "weekly_contract_deployers", "weekly_dev_core", "weekly_dev_sub"]
			cols_str = ", ".join(cols)
			placeholders = ", ".join(['%s'] * len(cols))
			update_cols = ", ".join([f"{col}=VALUES({col})" for col in cols[2:]])

			for i in range(0, len(art_sf_table), batch_size):
				cursor.executemany(f"INSERT INTO j_raw ({cols_str}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_cols}", art_sf_table[i:i+batch_size])
			print("Updated artemis (SF)")
			
			conn.commit()

			# Update sector info

			update_sectors = """
				UPDATE j_raw r
				JOIN v_all_unique_projects p ON p.project_id = r.project_name
				SET r.sector = p.sector
				WHERE r.sector IS NULL OR r.sector <> p.sector
			"""
			cursor.execute(update_sectors)
			conn.commit()

	except Error as e:
		print(f"Error: {e}")
