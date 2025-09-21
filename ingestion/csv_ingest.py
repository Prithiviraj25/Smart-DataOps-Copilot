import os
import sys
import uuid
import logging
import json
from db_utils.db_manager import insert_metadata,create_table
from pyspark.sql import DataFrame,SparkSession, DataFrame
from pyspark.sql.functions import col, trim, when
from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, FloatType, BooleanType
from dotenv import load_dotenv

# ------------------ Load environment ------------------
load_dotenv()

# ------------------ Logging setup ------------------
LOG_FILE = "main.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("csv_ingest")

# ------------------ Spark session ------------------
spark = SparkSession.builder \
    .appName("CSV Ingest") \
    .config("spark.jars", "/Users/prithivi/Documents/Data Engineering/Smart-DataOps-Copilot/jdbc_driver/postgresql-42.7.7.jar") \
    .getOrCreate()

POSTGRES_URL =os.getenv("JDBC_POSTGRES_URL")

# ------------------ Functions ------------------
def read_existing_table(table_name: str) -> DataFrame:
    """
    Read existing Postgres table into a Spark DataFrame.
    Returns None if table does not exist.
    """
    try:
        df_existing = spark.read \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", table_name) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        logger.info(f"ingestion/csv_ingest: read_existing_table: Existing table '{table_name}' loaded. Rows: {df_existing.count()}")
        return df_existing
    except Exception as e:
        logger.warning(f"ingestion/csv_ingest: read_existing_table: Table '{table_name}' does not exist yet: {e}")
        return None
    


def clean_dataframe(df: DataFrame) -> DataFrame:
    """
    Clean Spark DataFrame:
    - Convert empty strings to NULL in all columns
    - Trim string columns
    - Drop duplicate rows
    - Drop rows with NULLs
    Schema-aware: handles numeric and boolean columns correctly
    """
    try:
        # 1. Convert empty strings to NULL
        for field in df.schema.fields:
            col_name = field.name
            dtype = field.dataType

            if isinstance(dtype, StringType):
                # Trim strings and convert empty strings to NULL
                df = df.withColumn(col_name, when(trim(col(col_name)) == "", None).otherwise(trim(col(col_name))))
            
            elif isinstance(dtype, IntegerType):
                # Convert invalid or empty integer strings to NULL
                df = df.withColumn(col_name, when(~col(col_name).cast("int").isNotNull(), None).otherwise(col(col_name).cast("int")))
            
            elif isinstance(dtype, LongType):
                df = df.withColumn(col_name, when(~col(col_name).cast("bigint").isNotNull(), None).otherwise(col(col_name).cast("bigint")))
            
            elif isinstance(dtype, FloatType) or isinstance(dtype, DoubleType):
                df = df.withColumn(col_name, when(~col(col_name).cast("double").isNotNull(), None).otherwise(col(col_name).cast("double")))
            
            elif isinstance(dtype, BooleanType):
                df = df.withColumn(col_name, when(~col(col_name).cast("boolean").isNotNull(), None).otherwise(col(col_name).cast("boolean")))
            
            else:
                # Fallback for other types
                df = df.withColumn(col_name, col(col_name))

        # 2. Drop rows with NULL values
        df = df.na.drop()

        # 3. Drop duplicate rows
        df = df.dropDuplicates()

        return df

    except Exception as e:
        logger.error(f"ingestion/csv_ingest: clean_dataframe: Data cleaning failed: {e}")
        return None
    

def ingest_csv(file_path: str, table_name: str = None):
    """
    Ingest CSV into Postgres with duplicate check, create table with primary key if needed,
    and update metadata.
    """
    try:
        # Read CSV
        df_new = spark.read.csv(file_path, header=True, inferSchema=True)
        sanitized_cols = [col.replace(" ", "_").lower() for col in df_new.columns]
        df_new = df_new.toDF(*sanitized_cols)
        df_new = clean_dataframe(df_new)
        if(df_new is None):
            raise Exception("Data cleaning failed.")
        logger.info(f"ingestion/csv_ingest: ingest_csv: Cleaned CSV loaded: {df_new.count()} rows, {len(df_new.columns)} columns")

        # Generate table name if not provided
        if not table_name:
            table_name = f"dataset_{uuid.uuid4().hex[:8]}"

        # Infer schema mapping Spark types → Postgres types
        spark_schema = {}
        for field in df_new.schema.fields:
            if "StringType" in str(field.dataType):
                spark_schema[field.name] = "VARCHAR(255)"
            elif "IntegerType" in str(field.dataType):
                spark_schema[field.name] = "INT"
            elif "LongType" in str(field.dataType):
                spark_schema[field.name] = "BIGINT"
            elif "DoubleType" in str(field.dataType) or "FloatType" in str(field.dataType):
                spark_schema[field.name] = "FLOAT"
            elif "BooleanType" in str(field.dataType):
                spark_schema[field.name] = "BOOLEAN"
            else:
                spark_schema[field.name] = "TEXT"  # fallback

        # Check for existing table
        df_existing = read_existing_table(table_name)

        if df_existing:
            # Only compare matching columns
            common_cols = [col for col in df_new.columns if col in df_existing.columns]
            df_to_write = df_new.select(*common_cols).exceptAll(df_existing.select(*common_cols))
            rows_to_write = df_to_write.count()
            if rows_to_write == 0:
                logger.info("ingestion/csv_ingest: ingest_csv: No new rows to write. Exiting.")
                return
            logger.info(f"ingestion/csv_ingest: ingest_csv: Rows to append after duplicate check: {rows_to_write}")
        else:
            # Table does not exist → create with primary key
            create_table(table_name, spark_schema)
            df_to_write = df_new
            rows_to_write = df_to_write.count()
            logger.info(f"ingestion/csv_ingest: ingest_csv: Table created. Rows to write: {rows_to_write}")

        # 
        try:
            df_to_write.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", table_name) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
            logger.info(f"ingestion/csv_ingest: ingest_csv: Successfully written {rows_to_write} rows to table '{table_name}'")
        except Exception as e:
            logger.error(f"ingestion/csv_ingest: ingest_csv: Failed to write data to Postgres: {e}")
            raise

        # Update metadata
        schema_json = json.loads(df_new.schema.json())
        null_counts = {col: df_new.filter(df_new[col].isNull()).count() for col in df_new.columns}
        dataset_id = table_name.split("_")[-1]

        insert_metadata(dataset_id, table_name, df_new.count(), schema_json, null_counts)
        logger.info(f"ingestion/csv_ingest: ingest_csv: Metadata updated for dataset_id={dataset_id}")

    except Exception as e:
        logger.error(f"ingestion/csv_ingest: ingest_csv: CSV ingestion failed: {e}")
        raise

# ------------------ CLI / Subprocess ------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python csv_ingest.py <csv_file_path> [table_name]")
        sys.exit(1)

    csv_file = sys.argv[1]
    table_name_arg = sys.argv[2] if len(sys.argv) > 2 else None

    try:
        ingest_csv(csv_file, table_name_arg)
        sys.exit(0)  # success
    except Exception as e:
        print(f"CSV ingestion failed: {e}")
        sys.exit(1)  # failure