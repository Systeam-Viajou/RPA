import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

conn_db1 = psycopg2.connect(os.getenv('DB1_URL'))
conn_db2 = psycopg2.connect(os.getenv('DB2_URL'))