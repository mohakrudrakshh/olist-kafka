
import os

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema=os.environ["SNOWFLAKE_SCHEMA"],
)

try:
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT CURRENT_VERSION()")
        print("Snowflake connected successfully!")
        print("Snowflake version:", cursor.fetchone()[0])

        cursor.execute("SHOW TABLES IN SCHEMA OLIST_ECOMMERCE.CLEAN")
        print("\nAvailable CLEAN tables:")

        for row in cursor.fetchall():
            print("-", row[1])

    finally:
        cursor.close()

finally:
    conn.close()
