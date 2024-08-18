from m_tt import *
from m_art import *
from m_bitformance import *
from m_functions import *
from m_fred import *
from m_calculations import *
from m_cq import *
db_name = os.getenv("DB_NAME")
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("LOCAL_DB_PASSWORD")
counter = 1


update_artemis = False
update_token_terminal = False
update_bitformance = True
update_fred = False
update_smas = False
update_cq = False
conn = mysql.connector.connect(host="localhost", database=db_name, user=db_username, password=db_password)

try:
	if conn.is_connected():
		cursor = conn.cursor()
		print("Connected to DB ^.^")
		new_log_entry(conn, ("g", "Core", f"Starting script"))
		
		if update_artemis:
			# ============ ARTEMIS ============
			# Update all projects list
			# art_update_all_projects_list(cursor, conn, api_query)
			# new_log_entry(conn, ("g", "Core", "Step 1/11"))

			# # Get all available metric keys
			# art_get_api_project_metrics(cursor, conn, api_query)
			# new_log_entry(conn, ("g", "Core", "Step 2/11"))

			# # Add wanted metrics to db from list
			# art_add_selected_metrics(cursor, conn)
			# new_log_entry(conn, ("g", "Core", "Step 3/11"))

			# # Update unique metrics table with every unique metric key
			# art_update_unique_metrics_table(cursor, conn)
			# new_log_entry(conn, ("g", "Core", "Step 4/11"))

			# Then update metric data itself
			art_update_metric_data(cursor, conn, art_api_key)
			new_log_entry(conn, ("g", "Core", "Step 5/11"))

			# Optional, update ecosystems (not projects)
			#art_update_supported_ecosystems_from_api(cursor, conn, api_query)


		if update_token_terminal:
			# ============ TOKEN TERMINAL ============
			# Get a list of all market sectors
			# tt_update_all_market_sectors_list(cursor, conn, tt_api_key)
			# new_log_entry(conn, ("g", "core", "Step 6/11"))

			# If there is a new metric, do not update, if there is no difference, update
			# new_log_entry(conn, ("g", "Core", "Step 7/11"))
			# tt_update_project_list(cursor, conn, tt_api_key)
   
			# Apply market sector to project list
			# tt_update_project_ids_with_market_sector(cursor, conn, tt_api_key)
			# new_log_entry(conn, ("g", "core", "Step 8/11"))

			diff = tt_compare_available_metrics(conn)
			
			if diff == []:
				tt_update_project_list(cursor, conn, tt_api_key)
				new_log_entry(conn, ("g", "Core", "Step 9/11"))
				tt_update_project_metrics(cursor, conn, tt_api_key)
				new_log_entry(conn, ("g", "Core", "Step 10/11"))
			else:
				print("Uh Oh, new metric available!")
				new_log_entry(conn, ("h", "Token Terminal", f"New Metric, Can not continue, difference: {diff}"))
				print(diff)

		if update_bitformance:
			bf_update_data(conn)

		if update_fred:
			fred_update_data(conn)
		
		if update_smas:
			calculate_sma(conn)
		
		if update_cq:
			# Update exchange list
			# cq_update_exchange_data(conn)

			# ===== Update Exchange Flows ===== 
			cq_update_test(conn)

	else:
		print("Conn not connected")

except Error as e:
	print("Error: ", e)
	new_log_entry(conn, ("h", "Core", f"Error: {e}"))

finally:
	# if conn.is_connected():
	# 	new_log_entry(conn, ("g", "Core", "Step 11/11"))
	# 	new_log_entry(conn, ("l", "Core", "FINISHED"))
	# if sf_conn.is_connected:
		# sf_conn.close()
	# cursor.close()
	# conn.close()
	# print("MySQL connection is closed")
	# print("Done")
	pass


