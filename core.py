from concurrent.futures import ThreadPoolExecutor, as_completed
from mysql.connector import pooling

from m_tt import *
from m_art import *
from m_bitformance import *
from m_functions import *
from m_fred import *
from m_calculations import *
from m_cq_bitcoin import *
from m_art_snowflake import *
from m_defi_llama import *

from f_defi_agg_metrics import *
from f_defi_assets_metrics import *
from f_scp_agg_metrics import *
from f_scp_assets_metrics import *

# from m_staking_rewards import *
from sqlalchemy import create_engine
db_name = os.getenv("DB_NAME")
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("LOCAL_DB_PASSWORD")


# TT & Art project list
update_project_list = False

# Artemis
update_art_sf = True
update_artemis = True

# Token Terminal
update_token_terminal = True

# Bitformance
update_bitformance = True

# Defi Llama
update_defi_llama = True

# FRED
update_fred = False

# Crypto Quant
update_cq = True

# Staking Rewards
update_sr = False

# Calculations
update_main_table = True
update_fact_table = True


db_conn = {'host':"localhost", 'database':db_name, 'user':db_username, 'password':db_password, 'port':3303}
single_conn = mysql.connector.connect(host="localhost", database=db_name, user=db_username, password=db_password, port=3303)
conn_sf = snowflake.connector.connect(user='conordb', password=sf_db_pw, account=sf_acc_id, warehouse='COMPUTE_WH', database='ARTEMIS_DATA')
engine = sqlalchemy.create_engine(f'mysql+mysqlconnector://{db_username}:{db_password}@localhost:3303/helios')
engine_sf = sqlalchemy.create_engine(f"snowflake://conordb:{sf_db_pw}@{sf_acc_id}/ARTEMIS_DATA/AAVE?warehouse=COMPUTE_WH&role=ACCOUNTADMIN")

pool = pooling.MySQLConnectionPool(pool_name='main_pool', pool_size=10, **db_conn)

def update_art_sf_data():
	global update_art_sf, pool, conn_sf
	conn = pool.get_connection()
	try:
		if update_art_sf:
			sf_cursor = conn_sf.cursor()
			sf_cursor.execute("SELECT 1")
			new_log_entry(conn, ("g", "Core", "Beginning update for SF Artemis"))
			sf_art_update_raw_data(conn_sf, conn)
			art_sf_get_fee_data(conn)
			new_log_entry(conn, ("g", "Core", "finished update for SF Artemis successfully"))
	finally:
		conn.close()

def update_artemis_data():
	global update_artemis, pool
	conn = pool.get_connection()
	
	try:
		if update_artemis:
			# ============ ARTEMIS ============
			# Update all projects list
			# art_update_all_projects_list(conn, api_query)

			# # Get all available metric keys
			# art_get_api_project_metrics(conn, api_query)

			# # Add wanted metrics to db from list
			# art_add_selected_metrics(conn)

			# # Update unique metrics table with every unique metric key
			# art_update_unique_metrics_table(conn)

			# Then update metric data itself
			new_log_entry(conn, ("g", "Core", "Beginning update for artemis data"))
			art_update_metric_data(conn, art_api_key)
			new_log_entry(conn, ("g", "Core", "Finished update for artemis data successfully"))

			# Optional, update ecosystems (not projects)
			#art_update_supported_ecosystems_from_api(conn, api_query)
	finally:
		conn.close()


def update_bitformance_data():
	global update_bitformance, pool
	conn = pool.get_connection()

	try:
		if update_bitformance:
			new_log_entry(conn, ("g", "Core", "Beginning update for bitformance data"))
			bf_update_data(conn)
			new_log_entry(conn, ("g", "Core", "Finished update for bitformance successfully"))
	finally:
		conn.close()


def update_fred_data():
	global update_fred, pool
	conn = pool.get_connection()

	try:
		if update_fred:
			new_log_entry(conn, ("g", "Core", "Beginning update for fred data"))
			fred_update_data(conn)
			new_log_entry(conn, ("g", "Core", "Finished update for fred successfully"))
	finally:
		conn.close()


def update_defi_llama_data():
	global update_defi_llama, pool
	conn = pool.get_connection()

	try:
		if update_defi_llama:
			new_log_entry(conn, ("g", "Core", "Beginning update for Defi Llama data"))
			dl_update_defi_llama_tables(conn)
			dl_update_overview_yield(engine)
			new_log_entry(conn, ("g", "Core", "Finished update for Defi Llama successfully"))
	finally:
		conn.close()


def update_cq_data():
	global update_cq, pool
	conn = pool.get_connection()

	try:
		if update_cq:
			new_log_entry(conn, ("g", "Core", "Beginning update for cq data"))
			# Update exchange list
			# cq_update_exchange_data(conn)
			# ===== Update Exchange Flows ===== 
			cq_btc_cq_update_bitcoin(conn)
			new_log_entry(conn, ("g", "Core", "Finished update for cq successfully"))
	finally:
		conn.close()


def update_sr_data():
	global update_sr, pool
	conn = pool.get_connection()

	try:
		if update_sr:
			new_log_entry(conn, ("g", "Core", "Beginning update for cq data"))
			sr_update_raw_data(conn)
			new_log_entry(conn, ("g", "Core", "Finished update for cq successfully"))
	finally:
		conn.close()


def update_token_terminal_data():
	global update_token_terminal, pool
	conn = pool.get_connection()

	try:
		if update_token_terminal:
			new_log_entry(conn, ("g", "Core", "Beginning update for token terminal"))
			# ============ TOKEN TERMINAL ============
			# Get a list of all market sectors
			tt_update_all_market_sectors_list(conn, tt_api_key)

			# Apply market sector to project list
			tt_update_project_ids_with_market_sector(conn, tt_api_key)

			tt_update_raw_data(conn, tt_api_key)
			new_log_entry(conn, ("g", "Core", "finished update for tt successfully"))
	finally:
		conn.close()

		
# Core stuff
try:
	new_log_entry(single_conn, ("g", "Core", f"Beginning updates for {'Artemis (API),' if update_artemis else ''} {'Artemis (SF),' if update_art_sf else ''} {'TT,' if update_token_terminal else ''} {'Bitformance,' if update_bitformance else ''} {'FRED,' if update_fred else ''} {'CQ,' if update_cq else ''}  {'Fact Table,' if update_fact_table else ''}  {'Main Table' if update_main_table else ''}"))

	if update_project_list:
		# bf_get_coin_info(single_conn)
		art_update_all_projects_list(single_conn)
		tt_update_project_list(single_conn, tt_api_key)

	tasks = [
		update_art_sf_data,
		update_artemis_data,
		update_bitformance_data,
		update_fred_data,
		update_defi_llama_data,
		update_cq_data,
		update_sr_data,
		update_token_terminal_data
	]

	with ThreadPoolExecutor(max_workers=10) as executor:
		futures = {executor.submit(task): task.__name__ for task in tasks}

		for future in as_completed(futures):
			try:
				future.result()
			except Exception as e:
				new_log_entry(single_conn, ("h", "Core", f"Error in {futures[future]}: {e}"))


	if update_main_table:
		new_log_entry(single_conn, ("g", "Core", "Beginning update for main table data"))
		calc_update_raw_table(single_conn, engine)
		new_log_entry(single_conn, ("g", "Core", "Finished update for main table successfully"))

	if update_fact_table:
		new_log_entry(single_conn, ("g", "Core", "Beginning update for Fact Table data"))
		save_to_fact_table_defi(single_conn) # f_defi_assets_metrics
		save_to_fact_table_scp(single_conn) # f_scp_assets_metrics
		save_to_fact_table_agg_defi_agg(single_conn) #f_defi_agg_metrics
		save_to_fact_table_agg_scp_agg(single_conn) # f_scp_agg_metrcics
		new_log_entry(single_conn, ("g", "Core", "Finished update for Fact table successfully"))

	if single_conn.is_connected():
		cursor = single_conn.cursor()

		# final bits
		print('LUC - removing f_universe_assets_metrics table')
		cursor.execute("DROP TABLE IF EXISTS f_universe_assets_metrics")
		single_conn.commit()
		print('LUC - remaking f_universe_assets_metrics table')

		cursor.execute("""
				CREATE TABLE f_universe_assets_metrics
					SELECT 
						f_defi_assets_metrics.*, 
						'DeFi' AS `Table`
					FROM f_defi_assets_metrics
					UNION ALL
					SELECT 
						f_scp_assets_metrics.*, 
						'SCP' AS `Table`
					FROM f_scp_assets_metrics;
                """)
		single_conn.commit()

		print('get sr data into j_raw')
		cursor.execute("""
		UPDATE j_raw j
		JOIN project_mapping pm ON j.project_name = pm.j_raw  -- Match project names using mapping table
		JOIN sr_raw_data sr ON pm.staking_rewards = sr.project_id AND j.datestamp = sr.datestamp
		SET 
			j.sr_active_validators = sr.active_validators,
			j.sr_annualized_rewards_usd = sr.annualized_rewards_usd,
			j.sr_circulating_percentage = sr.circulating_percentage,
			j.sr_daily_trading_volume = sr.daily_trading_volume,
			j.sr_delegated_tokens = sr.delegated_tokens,
			j.sr_inflation_rate = sr.inflation_rate,
			j.sr_real_reward_rate = sr.real_reward_rate,
			j.sr_reward_rate = sr.reward_rate,
			j.sr_staked_tokens = sr.staked_tokens,
			j.sr_staking_marketcap = sr.staking_marketcap,
			j.sr_staking_ratio = sr.staking_ratio,
			j.sr_total_staking_wallets = sr.total_staking_wallets,
			j.sr_total_validators = sr.total_validators,
			j.sr_circulating_supply = sr.circulating_supply
		""")
		single_conn.commit()

		print("updating j_raw with dl chain fees")

		cursor.execute("""
		SELECT DISTINCT chain FROM dl_calcs;
		""")
		chains = [row[0] for row in cursor.fetchall()]

		update_query = """
		UPDATE j_raw
		JOIN project_mapping ON j_raw.project_name = project_mapping.j_raw
		JOIN dl_calcs ON project_mapping.defi_llama = dl_calcs.chain AND j_raw.datestamp = dl_calcs.datestamp
		SET 
		j_raw.dl_chain_fees_1000 = dl_calcs.fees_over_1000,
		j_raw.dl_dapp_count_1000 = dl_calcs.count_over_1000,
		j_raw.dl_chain_fees_raw = dl_calcs.fees_over_0,
		j_raw.dl_dapp_count_raw = dl_calcs.count_over_0
		"""

		cursor.execute(update_query)
		single_conn.commit()




	else:
		print("Conn not connected")

except Error as e:
	print("Error: ", e)
	new_log_entry(single_conn, ("h", "Core", f"Error: {e}"))

except Exception as e:
	print(f"Exception error: {e}")
	new_log_entry(single_conn, ("h", "Core", f"Error: {e}"))

finally:
	new_log_entry(single_conn, ("g", "Core", "FINISHED"))
	
	# if single_conn.is_connected():
	# 	single_conn.close()

	print("MySQL connection is closed")
	print("Done")







