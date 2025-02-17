import pandas as pd, logging, numpy as np
from datetime import datetime
from mysql.connector import Error

# Example usage
# new_log_entry(conn, ("G", "source," "Test message, action completed successfully"))
def new_log_entry(conn, log_data):
	try:
		with conn.cursor() as cursor:
			CURRENT_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			warning_lvl, from_source, message = log_data
			warning_lvl = warning_lvl.upper()
			message = message.strip()
			from_source = from_source.strip()
			cursor.execute("INSERT INTO cb_log (timestamp, from_source, warning_lvl, message) VALUES (%s, %s, %s, %s)", (CURRENT_TIMESTAMP, from_source, warning_lvl, message))
			conn.commit()
	except Error as e:
		print(f"Error in log entry system: {e}")
	except Exception as ex:
		print(f"Unexpected error: {ex}")



def udb(conn, insert_type, table_name, amt_of_unique_cols, df):
	"""
	Updates or inserts data into the specified database table.

	Parameters:
	- conn: Database connection object.
	- insert_type (str): Type of insertion ('update' or 'ignore').
	- table_name (str): Name of the database table.
	- amt_of_unique_cols (int): Number of unique columns used for identifying records.
	- df (pd.DataFrame): DataFrame containing the data to insert/update.

	Raises:
	- ValueError: If amt_of_unique_cols is not an integer or if the DataFrame headers are invalid.
	- TypeError: If df is not a pandas DataFrame.
	"""
	
	batch_size = 5000

	# Making sure df is a df and has headers
	if not isinstance(df, pd.DataFrame):
		raise TypeError("df must be a pandas DataFrame")
	if not all(isinstance(col, str) and col != '' for col in df.columns):
		raise ValueError("df needs column headers")

	# Making sure the amt_of_unique_cols is an int
	if not isinstance(amt_of_unique_cols, int):
		raise ValueError("amt_of_unique_cols must be an int")

	# Transforming the data
	db_columns = df.columns.tolist()
	column_names_str = ", ".join(db_columns)
	placeholders = ", ".join(['%s'] * len(db_columns))
	update_columns = ", ".join([f"{col}=VALUES({col})" for col in db_columns[amt_of_unique_cols:]])


	if insert_type == "update":
		query = f"INSERT INTO {table_name} ({column_names_str}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_columns}"
	elif insert_type == "ignore":
		query = f"INSERT IGNORE INTO {table_name} ({column_names_str}) VALUES ({placeholders})"
	else:
		raise ValueError("Invalid insert type")

	df = df.replace({np.nan: None})
	total_rows = len(df)

	try:
		with conn.cursor() as cursor:
			for start in range(0, total_rows, batch_size):
				end = start + batch_size
				batch_values = list(df.iloc[start:end].itertuples(index=False, name=None))

				cursor.executemany(query, batch_values)
				affected_rows = cursor.rowcount

				conn.commit()
				logging.info(f"Data updated successfully, affected rows: {affected_rows}")

	except Exception as e:
		logging.error("An error occurred during commit: %s", e)
		new_log_entry(conn, ("h", "update function", f"An error occurred during commit: {e}"))
	


def udbOLD(conn, insert_type, table_name, amt_of_unique_cols, df):
	"""
	Updates or inserts data into the specified database table.

	Parameters:
	- conn: Database connection object.
	- insert_type (str): Type of insertion ('update' or 'ignore').
	- table_name (str): Name of the database table.
	- amt_of_unique_cols (int): Number of unique columns used for identifying records.
	- df (pd.DataFrame): DataFrame containing the data to insert/update.

	Raises:
	- ValueError: If amt_of_unique_cols is not an integer or if the DataFrame headers are invalid.
	- TypeError: If df is not a pandas DataFrame.
	"""
	
	# Making sure df is a df and has headers
	if not isinstance(df, pd.DataFrame):
		raise TypeError("df must be a pandas DataFrame")
	if not all(isinstance(col, str) and col != '' for col in df.columns):
		raise ValueError("df needs column headers")

	# Making sure the amt_of_unique_cols is an int
	if not isinstance(amt_of_unique_cols, int):
		raise ValueError("amt_of_unique_cols must be an int")

	# Transforming the data
	db_columns = df.columns.tolist()
	column_names_str = ", ".join(db_columns)
	placeholders = ", ".join(['%s'] * len(db_columns))
	update_columns = ", ".join([f"{col}=VALUES({col})" for col in db_columns[amt_of_unique_cols:]])


	if insert_type == "update":
		query = f"INSERT INTO {table_name} ({column_names_str}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_columns}"
	elif insert_type == "ignore":
		query = f"INSERT IGNORE INTO {table_name} ({column_names_str}) VALUES ({placeholders})"
	else:
		raise ValueError("Invalid insert type")

	df = df.replace({np.nan: None})
	values = list(df.itertuples(index=False, name=None))

	all_columns = df.columns.tolist()

	try:
		with conn.cursor() as cursor:
			cursor.executemany(query, values)
			affected_rows = cursor.rowcount
			conn.commit()
			logging.info(f"Data updated successfully, affected rows: {affected_rows}")
	except Exception as e:
		logging.error("An error occurred during commit: %s", e)
		new_log_entry(conn, ("h", "token terminal", f"An error occurred during commit: {e}"))
	