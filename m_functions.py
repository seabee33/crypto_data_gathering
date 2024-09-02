# Function to make log entries into local db

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
