import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
import logging
load_dotenv()

#--- Setting up logging
LOG_FILE = "main.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a"),  
        logging.StreamHandler()                   
    ]
)
logger = logging.getLogger("db_manager")

POSTGRES_CONFIG = {
    "host": os.getenv("PGSQL_HOST"),
    "port": os.getenv("PGSQL_PORT", "5432"),
    "dbname": os.getenv("PGSQL_DBNAME"),
    "user": os.getenv("PGSQL_USER"),
    "password": os.getenv("PGSQL_PASSWORD")
}

#--- Function to get a connection to the PostgreSQL database
def get_connection():
    '''function to connect to the Database'''
    try:
        connection=psycopg2.connect(**POSTGRES_CONFIG)
        logger.infor("db_utils/db_manager.py:  get_connection: Successfully connected to the database")
        return connection
    except Exception as e:
        logger.error(f"db_utils/db_manager.py:  get_connection: Error connecting to the database: {e}")
        raise

#----Initializing the metadata table
def init_metadata_table():
    '''function to initialize metadata table'''
    try:
        connection=get_connection()
        cursor=connection.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS datasets_metadata (
                dataset_id VARCHAR(50) PRIMARY KEY,
                table_name VARCHAR(100),
                row_count BIGINT,
                schema JSONB,
                null_counts JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        connection.commit()
        cursor.close()
        connection.close()
        logger.info("db_utils/db_manager.py:  init_metadata_table: Metadata table initialized successfully")
    except Exception as e:
        logger.error(f"db_utils/db_manager.py:  init_metadata_table: Error initializing metadata table: {e}")
        raise

#--- Function to insert metadata into the metadata table
def insert_metadata(dataset_id, table_name, row_count, schema, null_counts):
    try:
        connection=get_connection()
        cursor=connection.cursor()
        cursor.execute("""
                INSERT INTO datasets_metadata (dataset_id, table_name, row_count, schema, null_counts)
                            VALUES (%s, %s, %s, %s::jsonb, %s::jsonb)
                            ON CONFLICT (dataset_id) DO UPDATE
                            SET row_count = EXCLUDED.row_count,
                                schema = EXCLUDED.schema,
                                null_counts = EXCLUDED.null_counts;
                        """, (dataset_id, table_name, row_count, str(schema), str(null_counts))
                    )
        connection.commit()
        cursor.close()
        connection.close()
        logger.info("db_utils/db_manager.py:  insert_metadata: Metadata inserted/updated successfully")
    except Exception as e:
        logger.error(f"db_utils/db_manager.py:  insert_metadata: Error inserting/updating metadata: {e}")
        raise
        