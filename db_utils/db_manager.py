import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
import logging
import json
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


#--- Function to get a connection to the PostgreSQL database
def get_connection():
    '''function to connect to the Database'''
    try:
        connection=psycopg2.connect(os.getenv("PGSQL_URL"))
        logger.info("db_utils/db_manager.py:  get_connection: Successfully connected to the database")
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
                        """, (dataset_id, table_name, row_count, json.dumps(schema), json.dumps(null_counts))
                    )
        connection.commit()
        cursor.close()
        connection.close()
        logger.info("db_utils/db_manager.py:  insert_metadata: Metadata inserted/updated successfully")
    except Exception as e:
        logger.error(f"db_utils/db_manager.py:  insert_metadata: Error inserting/updating metadata: {e}")
        raise
        
#-----function to create a new table for ingested file
def create_table(table_name: str, schema: dict):
    try:
        connection=get_connection()
        cursor=connection.cursor()
        # Convert schema dict to column definitions
        col_defs = ", ".join([f"{col} {dtype}" for col, dtype in schema.items()])

        # SQL for creating table with primary key 'id'
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id BIGSERIAL PRIMARY KEY,
            {col_defs}
        );
        """
        )
        connection.commit()
        cursor.close()
        connection.close()
        logger.info(f"db_utils/db_manager.py:  create_table: Table name:{table_name} created successfully")
        return True
    except Exception as e:  
        logger.error(f"db_utils/db_manager.py:  create_table: Error creating table {table_name}: {e}")
        raise