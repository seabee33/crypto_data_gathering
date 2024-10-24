from m_tt import *
from m_art import *
from m_bitformance import *
from m_functions import *
from m_fred import *
from m_calculations import *
from m_cq import *
from m_art_snowflake import *
from m_defi_llama import *
from m_fact_table import *
from sqlalchemy import create_engine
db_name = os.getenv("DB_NAME")
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("LOCAL_DB_PASSWORD")

# Artemis
update_art_sf = False
update_artemis = False

# Token Terminal
update_token_terminal = True

# Bitformance
update_bitformance = False

# Defi Llama
update_defi_llama = False

# FRED
update_fred = False

# Crypto Quant
update_cq = False

# Staking Rewards
update_sr = False

# Calculations
update_main_table = False
update_fact_table = False


conn = mysql.connector.connect(host="localhost", database=db_name, user=db_username, password=db_password, port=3303)
conn_sf = snowflake.connector.connect(user='conordb', password=sf_db_pw, account=sf_acc_id, warehouse='COMPUTE_WH', database='ARTEMIS_DATA')
engine = sqlalchemy.create_engine(f'mysql+mysqlconnector://{db_username}:{db_password}@localhost:3303/helios')
engine_sf = sqlalchemy.create_engine(f"snowflake://conordb:{sf_db_pw}@{sf_acc_id}/ARTEMIS_DATA/AAVE?warehouse=COMPUTE_WH&role=ACCOUNTADMIN")


try:
	new_log_entry(conn, ("g", "Core", f"Beginning updates for {'Artemis (API),' if update_artemis else ''} {'Artemis (SF),' if update_art_sf else ''} {'TT,' if update_token_terminal else ''} {'Bitformance,' if update_bitformance else ''} {'FRED,' if update_fred else ''} {'CQ,' if update_cq else ''}  {'Fact Table,' if update_fact_table else ''}  {'Main Table' if update_main_table else ''}"))

	if update_art_sf:
		sf_cursor = conn_sf.cursor()
		sf_cursor.execute("SELECT 1")
		new_log_entry(conn, ("g", "Core", "Beginning update for SF Artemis"))
		sf_art_update_raw_data(conn_sf, conn)
		art_sf_get_fee_data(conn)
		new_log_entry(conn, ("g", "Core", "finished update for SF Artemis successfully"))

	if conn.is_connected():
		cursor = conn.cursor()
		
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

		if update_token_terminal:
			new_log_entry(conn, ("g", "Core", "Beginning update for token terminal"))
			# ============ TOKEN TERMINAL ============
			# Get a list of all market sectors
			# tt_update_all_market_sectors_list(cursor, conn, tt_api_key)

			# Apply market sector to project list
			# tt_update_project_ids_with_market_sector(cursor, conn, tt_api_key)

			# Update project list
			# tt_update_project_list(cursor, conn, tt_api_key)

			# tt_update_project_list(cursor, conn, tt_api_key)
			tt_update_raw_data(conn, tt_api_key)
			new_log_entry(conn, ("g", "Core", "finished update for tt successfully"))


		if update_bitformance:
			new_log_entry(conn, ("g", "Core", "Beginning update for bitformance data"))
			bf_update_data(conn)
			new_log_entry(conn, ("g", "Core", "Finished update for bitformance successfully"))

		if update_fred:
			new_log_entry(conn, ("g", "Core", "Beginning update for fred data"))
			fred_update_data(conn)
			new_log_entry(conn, ("g", "Core", "Finished update for fred successfully"))
		
		if update_defi_llama:
			new_log_entry(conn, ("g", "Core", "Beginning update for Defi Llama data"))
			# dl_update_defi_llama_tables(conn)
			dl_update_overview_yield(engine)
			new_log_entry(conn, ("g", "Core", "Finished update for Defi Llama successfully"))
	
		if update_cq:
			new_log_entry(conn, ("g", "Core", "Beginning update for cq data"))
			# Update exchange list
			# cq_update_exchange_data(conn)
			# ===== Update Exchange Flows ===== 
			cq_update(conn_cq)
			new_log_entry(conn, ("g", "Core", "Finished update for cq successfully"))

		if update_sr:
			new_log_entry(conn, ("g", "Core", "Beginning update for cq data"))
			sr_update_raw_data(conn)
			new_log_entry(conn, ("g", "Core", "Finished update for cq successfully"))

		if update_main_table:
			new_log_entry(conn, ("g", "Core", "Beginning update for main table data"))
			calc_update_raw_table(conn, engine)
			new_log_entry(conn, ("g", "Core", "Finished update for main table successfully"))

		if update_fact_table:
			new_log_entry(conn, ("g", "Core", "Beginning update for Fact Table data"))
			save_to_fact_table(conn)
			new_log_entry(conn, ("g", "Core", "Finished update for Fact table successfully"))

	else:
		print("Conn not connected")

except Error as e:
	print("Error: ", e)
	new_log_entry(conn, ("h", "Core", f"Error: {e}"))

finally:
	new_log_entry(conn, ("g", "Core", "FINISHED"))
	
	if conn.is_connected():
		conn.close()

	print("MySQL connection is closed")
	print("Done")
