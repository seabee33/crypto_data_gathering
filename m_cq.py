# Gets data from crypto quant api and inserts into local db
import requests, mysql.connector, json, time, os
from datetime import datetime, timedelta
from mysql.connector import Error
from dotenv import load_dotenv
from m_functions import *
load_dotenv()
cq_api_key = os.getenv("CRYPTO_QUANT_API_KEY")


def cq_update_exchange_data(conn):
	try:
		with conn.cursor() as cursor:
			response = requests.get("https://api.cryptoquant.com/v1/btc/status/entity-list?type=exchange", headers={"Authorization": f"Bearer {cq_api_key}"})
			data = response.json()

			exchange_list = []
			for exchange in data["result"]["data"]:
				exchange_info = (
					exchange['name'],
					exchange['symbol'],
					exchange['is_validated'],
					exchange['market_type'],
					exchange['is_spot'],
					exchange['is_derivative']
				)
				exchange_list.append(exchange_info)
			
			cursor.executemany("INSERT IGNORE INTO cq_exchanges (exchange_name, symbol, is_validated, market_type, is_spot, is_derivative) values (%s, %s, %s, %s, %s, %s)", exchange_list)
			conn.commit()
	except Error as e:
		print(f"CQ Error: {e}")


def cq_get_all_exchanges(conn):
	with conn.cursor() as cursor:
		cursor.execute("SELECT symbol FROM cq_exchanges ORDER BY symbol ASC")
		rows = cursor.fetchall()
		all_exchanges = [row[0] for row in rows]
		return all_exchanges


def cq_get_latest_date_for_thing(conn, table_name, exchange, col):
	with conn.cursor() as cursor:
		cursor.execute(f"SELECT datestamp FROM {table_name} WHERE exchange_symbol='{exchange}' and {col} IS NOT NULL ORDER BY datestamp DESC LIMIT 1")
		raw_data = cursor.fetchone()
		if raw_data is None:
			return None
		else:
			return raw_data[0]


def cq_update_test(conn):
	try:
		with conn.cursor() as cursor:
			current_date_m2 = (datetime.now() - timedelta(days=2)).strftime("%Y%m%d")
			cq_exchange_list = cq_get_all_exchanges(conn)

			cq_table_1 = "cq_btc_exchange_raw_data"
			cq_table_2 = "cq_btc_miner_raw_data"

			cq_params = {
				# Exchange Flows
				"reserve": {
					"url": "https://api.cryptoquant.com/v1/btc/exchange-flows/reserve",
					"need_exchange_list":True,
					"query_points":["reserve_usd", "reserve"],
					"db_query": f"INSERT INTO {cq_table_1} (datestamp, exchange_symbol, reserve_usd, reserve_btc) VALUES(%s, %s, %s, %s) ON DUPLICATE KEY UPDATE reserve_btc = VALUES(reserve_btc), reserve_usd = VALUES(reserve_usd)"
				},
				"netflow": {
					"url":"https://api.cryptoquant.com/v1/btc/exchange-flows/netflow",
					"need_exchange_list":True,
					"query_points":["netflow_total"],
					"db_query":f"INSERT INTO {cq_table_1} (datestamp, exchange_symbol, netflow_total) VALUES(%s, %s, %s) ON DUPLICATE KEY UPDATE netflow_total = VALUES(netflow_total)"
				},
				"inflow":{
					"url":"https://api.cryptoquant.com/v1/btc/exchange-flows/inflow",
					"need_exchange_list":True,
					"query_points":["inflow_total", "inflow_top10", "inflow_mean", "inflow_mean_ma7"],
					"db_query":f"INSERT INTO {cq_table_1} (datestamp, exchange_symbol, inflow_total, inflow_top10, inflow_mean, inflow_mean_ma7) VALUES(%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE inflow_total = VALUES(inflow_total),  inflow_top10 = VALUES(inflow_top10),  inflow_mean = VALUES(inflow_mean),  inflow_mean_ma7 = VALUES(inflow_mean_ma7)"
				},
				"outflow":{
					"url":"https://api.cryptoquant.com/v1/btc/exchange-flows/outflow",
					"need_exchange_list":True,
					"query_points":["outflow_total", "outflow_top10", "outflow_mean", "outflow_mean_ma7"],
					"db_query":f"INSERT INTO {cq_table_1} (datestamp, exchange_symbol, outflow_total, outflow_top10, outflow_mean, outflow_mean_ma7) VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE outflow_total = VALUES(outflow_total), outflow_top10 = VALUES(outflow_top10), outflow_mean = VALUES(outflow_mean), outflow_mean_ma7 = VALUES(outflow_mean_ma7)"
				},
				"transactions_count":{
					"url":"https://api.cryptoquant.com/v1/btc/exchange-flows/transactions-count",
					"need_exchange_list":True,
					"query_points":["transactions_count_inflow", "transactions_count_outflow"],
					"db_query": f"INSERT INTO {cq_table_1} (datestamp, exchange_symbol, transactions_count_inflow, transactions_count_outflow) VALUES(%s, %s, %s, %s) ON DUPLICATE KEY UPDATE transactions_count_inflow = VALUES(transactions_count_inflow), transactions_count_outflow = VALUES(transactions_count_outflow)"
				},
				"addresses-count":{
					"url":"https://api.cryptoquant.com/v1/btc/exchange-flows/addresses-count",
					"need_exchange_list":True,
					"query_points":["addresses_count_inflow", "addresses_count_outflow"],
					"db_query": f"INSERT INTO {cq_table_1} (datestamp, exchange_symbol, addresses_count_inflow, addresses_count_outflow) VALUES(%s, %s, %s, %s) ON DUPLICATE KEY UPDATE addresses_count_inflow = VALUES(addresses_count_inflow), addresses_count_outflow = VALUES(addresses_count_outflow)"
				},
				"in-house-flow":{
					"url":"https://api.cryptoquant.com/v1/btc/exchange-flows/in-house-flow",
					"need_exchange_list":True,
					"query_points":["flow_total", "flow_mean", "transactions_count_flow"],
					"db_query": f"INSERT INTO {cq_table_1} (datestamp, exchange_symbol, flow_total, flow_mean, transactions_count_flow) VALUES(%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE flow_total = VALUES(flow_total), flow_mean = VALUES(flow_mean), transactions_count_flow = VALUES(transactions_count_flow)"
				},
				# Flow indicator
				"miners-position-index":{
					"url":"https://api.cryptoquant.com/v1/btc/flow-indicator/mpi",
					"need_exchange_list":False,
					"query_points":["mpi"],
					"db_query":f"INSERT INTO {cq_table_2} (datestamp, exchange_symbol, mpi) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE mpi = VALUES(mpi)"
				},
				"exchange-whale-ratio":{
					"url":"https://api.cryptoquant.com/v1/btc/flow-indicator/exchange-whale-ratio",
					"need_exchange_list":True,
					"query_points":["exchange_whale_ratio"],
					"db_query":f"INSERT INTO {cq_table_1} (datestamp, exchange_symbol, exchange_whale_ratio) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE exchange_whale_ratio=VALUES(exchange_whale_ratio)"
				},
			}

			for endpoint, params in cq_params.items():
				if params["need_exchange_list"]:
					for exchange in cq_exchange_list:
						print(f"Checking {endpoint} data for {exchange}")

						last_update = cq_get_latest_date_for_thing(conn, cq_table_1, exchange, params["query_points"][0])
						last_update = "20100101" if last_update is None else (last_update - timedelta(days=10)).strftime("%Y%m%d")

						cq_data_to_update = []
						response = requests.get(f'{params["url"]}?window=day&from={last_update}&to={current_date_m2}&exchange={exchange}&limit=10000', headers={"Authorization": f"Bearer {cq_api_key}"})
						data = response.json()
						
						if data["status"]["code"] == 400:
							print(f"No data for exchange: {exchange}")
						elif data["status"]["code"] != 200:
							print("error fetching data")
						else:
							cq_data_to_update = [
								[item["date"], exchange] + [item[param] for param in params["query_points"]]
								for item in data["result"]["data"]
							]
							cursor.executemany(params["db_query"], cq_data_to_update)			
							conn.commit()
				elif not params["need_exchange_list"]:
					print(f"Checking {endpoint} data")

					last_update = cq_get_latest_date_for_thing(conn, cq_table_2, params["query_points"][0])
					last_update = "20100101" if last_update is None else (last_update - timedelta(days=10)).strftime("%Y%m%d")

					cq_data_to_update = []
					response = requests.get(f'{params["url"]}?window=day&from={last_update}&to={current_date_m2}&limit=10000', headers={"Authorization": f"Bearer {cq_api_key}"})
					data = response.json()
					
					if data["status"]["code"] == 400:
						print(f"No data for query: {endpoint}")
					elif data["status"]["code"] != 200:
						print("error fetching data")
					else:
						cq_data_to_update = [
							[item["date"]] + [item[param] for param in params["query_points"]]
							for item in data["result"]["data"]
						]
						cursor.executemany(params["db_query"], cq_data_to_update)			
						conn.commit()

	except Error as e:
		print(f"CQ Error: {e}")
	

# for debugging, ignore
# j_data = json.dumps(data, indent=4)
# with open ("data.json", "w") as file:
# 	file.write(j_data)

