import requests, mysql.connector, json, time, os, io, urllib.parse, sqlalchemy, gspread
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials

from mysql.connector import Error
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
from m_functions import *
import pandas as pd
load_dotenv()
g_sheets_url = os.getenv("G_SHEETS_URL")


scopes = ["https://www.googleapis.com/auth/spreadsheets"]
sheet_id = "1YSsIk2ghvPDHTcfUKkKo9cqg0XI_gSGh"
sheet_id = "1nyI3PRP-_RMQnRWEM4U-4Laqeh1Xx7dnRGYHh5bmMxs"
table_name = "Datafeed Woonomic!A4:S50"

creds = Credentials.from_service_account_file("gcreds.json", scopes=scopes)
client = gspread.authorize(creds)

sheet = client.open_by_key(sheet_id)

values = sheet.sheet1.row_values(1)
print(values)