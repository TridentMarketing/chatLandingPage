# Databricks notebook source
# MAGIC %md
# MAGIC # SocialIntents Chat Data Daily Pull
# MAGIC 
# MAGIC This notebook pulls chat histories from the SocialIntents REST API and saves to `silver.dechats` table.
# MAGIC 
# MAGIC **API Endpoint:** https://api.socialintents.com/v1/api/chats/
# MAGIC **Schedule:** Run daily to pull previous day's chat data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration Options

# COMMAND ----------

# ==== DATA PULL CONFIGURATION ====
# Set to True for daily incremental updates (yesterday only)
# Set to False for initial full 2025 year-to-date load
INCREMENTAL_MODE = False  # Change to True after initial load

print(f"Running in {'INCREMENTAL' if INCREMENTAL_MODE else 'FULL LOAD'} mode")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Setup and Configuration

# COMMAND ----------

import requests
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import base64
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. API Configuration

# COMMAND ----------

# SocialIntents API Configuration
# For Databricks: Use secrets or environment variables (recommended)
# Example: ACCOUNT_ID = dbutils.secrets.get(scope="socialintents", key="account_id")
# Example: API_TOKEN = dbutils.secrets.get(scope="socialintents", key="api_token")

# Default values (replace with your credentials or use Databricks secrets)
API_BASE_URL = "https://api.socialintents.com/v1/api/"
CHAT_ENDPOINT = "chats/"
ACCOUNT_ID = "2c9fa36b72b45bda0172c33055963258"  # Replace with your Account ID
API_TOKEN = "c2a8fba7-d05a-464a-b7e8-8a77005c7c5d"   # Replace with your API Token

# Create base64 encoded credentials for Basic Auth
credentials = base64.b64encode(f"{ACCOUNT_ID}:{API_TOKEN}".encode()).decode()

# Headers for API requests
headers = {
    "Authorization": f"Basic {credentials}",
    "Content-Type": "application/json"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Define Data Schema

# COMMAND ----------

# Schema for chat messages
message_schema = StructType([
    StructField("type", StringType(), True),
    StructField("nickname", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("body", StringType(), True)
])

# Schema for main chat data
chat_schema = StructType([
    StructField("id", StringType(), True),
    StructField("appId", StringType(), True),
    StructField("accountId", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("startDate", StringType(), True),
    StructField("userAgent", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("page", StringType(), True),
    StructField("visitorId", StringType(), True),
    StructField("visitorName", StringType(), True),
    StructField("visitorPhone", StringType(), True),
    StructField("visitorEmail", StringType(), True),
    StructField("agentName", StringType(), True),
    StructField("agentId", StringType(), True),
    StructField("duration", IntegerType(), True),
    StructField("visits", IntegerType(), True),
    StructField("messages", ArrayType(message_schema), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. API Data Retrieval Function

# COMMAND ----------

def get_chat_data(date_from=None, date_to=None, app_id=None, visitor_id=None, timezone="America/New_York"):
    """
    Retrieve chat data from SocialIntents API
    
    Parameters:
    - date_from: Start date (yyyy-MM-dd format)
    - date_to: End date (yyyy-MM-dd format)
    - app_id: Optional app ID filter
    - visitor_id: Optional visitor ID filter
    - timezone: Timezone (default: America/New_York)
    """
    
    # Build API URL
    url = f"{API_BASE_URL}{CHAT_ENDPOINT}"
    
    # Build parameters
    params = {}
    if date_from:
        params['date_from'] = date_from
    if date_to:
        params['date_to'] = date_to
    if app_id:
        params['app_id'] = app_id
    if visitor_id:
        params['visitor_id'] = visitor_id
    if timezone:
        params['timezone'] = timezone
    
    try:
        logger.info(f"Making API request to: {url}")
        logger.info(f"Parameters: {params}")
        
        response = requests.get(url, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get('apiStatus') == 'Success':
            logger.info(f"Successfully retrieved {len(data.get('chats', []))} chat records")
            return data.get('chats', [])
        else:
            logger.error(f"API Error: {data.get('apiStatusMessage', 'Unknown error')}")
            return []
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {str(e)}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON response: {str(e)}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return []

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Processing Functions

# COMMAND ----------

def process_chat_data(chat_records):
    """
    Process chat records and create DataFrame with flattened structure
    """
    if not chat_records:
        logger.warning("No chat records to process")
        return spark.createDataFrame([], chat_schema)
    
    try:
        # Create DataFrame from chat records
        df = spark.createDataFrame(chat_records, chat_schema)
        
        # Add processing metadata
        df = df.withColumn("ingestion_timestamp", current_timestamp()) \
               .withColumn("processing_date", current_date()) \
               .withColumn("message_count", size(col("messages")))
        
        # Convert startDate to proper timestamp (Spark 3.0+ compatible)
        df = df.withColumn("chat_start_timestamp", 
                          to_timestamp(col("startDate"), "yyyy-MM-dd HH:mm:ss a"))
        
        # Extract date from startDate for partitioning
        df = df.withColumn("chat_date", 
                          to_date(col("chat_start_timestamp")))
        
        logger.info(f"Processed {df.count()} chat records")
        return df
        
    except Exception as e:
        logger.error(f"Error processing chat data: {str(e)}")
        return spark.createDataFrame([], chat_schema)

def create_messages_df(chat_df):
    """
    Create a separate DataFrame for chat messages (normalized)
    """
    try:
        # Explode messages array to create one row per message
        messages_df = chat_df.select(
            col("id").alias("chat_id"),
            col("visitorId").alias("visitor_id"),
            col("agentId").alias("agent_id"),
            col("chat_date"),
            col("chat_start_timestamp"),
            explode(col("messages")).alias("message")
        ).select(
            col("chat_id"),
            col("visitor_id"),
            col("agent_id"),
            col("chat_date"),
            col("chat_start_timestamp"),
            col("message.type").alias("message_type"),
            col("message.nickname").alias("sender_nickname"),
            col("message.timestamp").alias("message_timestamp_ms"),
            col("message.body").alias("message_body")
        )
        
        # Convert timestamp from milliseconds to proper timestamp
        messages_df = messages_df.withColumn(
            "message_timestamp",
            from_unixtime(col("message_timestamp_ms") / 1000)
        )
        
        # Add processing metadata
        messages_df = messages_df.withColumn("ingestion_timestamp", current_timestamp())
        
        logger.info(f"Created messages DataFrame with {messages_df.count()} message records")
        return messages_df
        
    except Exception as e:
        logger.error(f"Error creating messages DataFrame: {str(e)}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Database Operations

# COMMAND ----------

def create_silver_dechats_table():
    """
    Create the silver.dechats table if it doesn't exist
    """
    try:
        # Create database if it doesn't exist
        spark.sql("CREATE DATABASE IF NOT EXISTS silver")
        
        # Create main chats table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS silver.dechats (
            id STRING,
            appId STRING,
            accountId STRING,
            referrer STRING,
            startDate STRING,
            userAgent STRING,
            ip STRING,
            page STRING,
            visitorId STRING,
            visitorName STRING,
            visitorPhone STRING,
            visitorEmail STRING,
            agentName STRING,
            agentId STRING,
            duration INT,
            visits INT,
            messages ARRAY<STRUCT<
                type: STRING,
                nickname: STRING,
                timestamp: BIGINT,
                body: STRING
            >>,
            ingestion_timestamp TIMESTAMP,
            processing_date DATE,
            message_count INT,
            chat_start_timestamp TIMESTAMP,
            chat_date DATE
        )
        USING DELTA
        PARTITIONED BY (chat_date)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """
        
        spark.sql(create_table_sql)
        logger.info("Created/verified silver.dechats table")
        
        # Note: Only creating the main dechats table (messages are nested within)
        logger.info("Single table approach: Messages stored as nested array in silver.dechats")
        
    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        raise

def save_to_silver_table(df, table_name, mode="append"):
    """
    Save DataFrame to silver table
    """
    try:
        # Check if table exists
        table_exists = spark.sql(f"SHOW TABLES IN silver").filter(f"tableName == '{table_name.split('.')[-1]}'").count() > 0
        
        if table_exists and mode == "append":
            # For existing tables, use overwriteSchema for messages table to handle schema conflicts
            if "messages" in table_name:
                df.write \
                  .format("delta") \
                  .mode("overwrite") \
                  .option("overwriteSchema", "true") \
                  .saveAsTable(table_name)
                logger.info(f"Overwritten {table_name} with {df.count()} records (schema updated)")
            else:
                df.write \
                  .format("delta") \
                  .mode(mode) \
                  .option("mergeSchema", "true") \
                  .saveAsTable(table_name)
                logger.info(f"Successfully appended {df.count()} records to {table_name}")
        else:
            # For new tables or overwrite mode
            df.write \
              .format("delta") \
              .mode(mode) \
              .option("mergeSchema", "true") \
              .saveAsTable(table_name)
            logger.info(f"Successfully saved {df.count()} records to {table_name}")
        
    except Exception as e:
        logger.error(f"Error saving to table {table_name}: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Main Execution Logic

# COMMAND ----------

def main():
    """
    Main function to execute chat data pull
    """
    try:
        # Always use yesterday as the end date to avoid partial day data
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        if INCREMENTAL_MODE:
            # Daily incremental mode - pull yesterday only
            start_date = yesterday
            end_date = yesterday
            logger.info(f"INCREMENTAL MODE: Pulling data for {yesterday}")
        else:
            # Full load mode - pull 2023 through yesterday (complete days only)
            start_date = "2023-01-01"
            end_date = yesterday
            logger.info(f"FULL LOAD MODE: Pulling data from {start_date} to {end_date} (complete days only)")
        
        # Create tables if they don't exist
        create_silver_dechats_table()
        
        # Pull chat data from 2025 year-to-date
        chat_records = get_chat_data(
            date_from=start_date,
            date_to=end_date,
            timezone="America/New_York"
        )
        
        if not chat_records:
            logger.warning("No chat data retrieved")
            return
        
        # Process the data
        chat_df = process_chat_data(chat_records)
        
        if chat_df.count() == 0:
            logger.warning("No processed chat data")
            return
        
        # Save main chat data with duplicate prevention
        if INCREMENTAL_MODE:
            # For incremental mode, remove existing data for the same date first
            yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            logger.info(f"Removing existing data for {yesterday_str} before appending new data")
            spark.sql(f"DELETE FROM silver.dechats WHERE chat_date = '{yesterday_str}'")
        else:
            # For full load mode, remove existing data for the date range being loaded
            logger.info(f"Removing existing data for date range {start_date} to {end_date} before loading")
            spark.sql(f"DELETE FROM silver.dechats WHERE chat_date >= '{start_date}' AND chat_date <= '{end_date}'")
            
        save_to_silver_table(chat_df, "silver.dechats")
        
        mode_text = "incremental" if INCREMENTAL_MODE else "full load"
        logger.info(f"Chat data pull completed successfully ({mode_text})")
        
        # Display summary
        display(chat_df.select("chat_date", "visitorName", "agentName", "duration", "message_count"))
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Execute Daily Pull

# COMMAND ----------

# Run the main function
main()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Data Quality Checks

# COMMAND ----------

# Verify data was loaded correctly
print("=== Data Quality Summary ===")
print(f"Total chat records in silver.dechats: {spark.table('silver.dechats').count()}")

# Remove the old messages table if it exists
try:
    spark.sql("DROP TABLE IF EXISTS silver.dechats_messages")
    print("Removed old silver.dechats_messages table")
except:
    pass

# Show recent data
print("\n=== Recent Chat Records ===")
display(spark.sql("""
    SELECT chat_date, COUNT(*) as chat_count, 
           SUM(message_count) as total_messages,
           COUNT(DISTINCT visitorId) as unique_visitors
    FROM silver.dechats 
    WHERE chat_date >= date_sub(current_date(), 7)
    GROUP BY chat_date 
    ORDER BY chat_date DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Optional: Manual Date Range Pull
# MAGIC 
# MAGIC Uncomment and modify the code below to pull data for a specific date range

# COMMAND ----------

# # Manual date range pull (uncomment to use)
# def manual_date_pull(start_date, end_date):
#     """
#     Pull data for a specific date range
#     Format: 'YYYY-MM-DD'
#     """
#     logger.info(f"Manual pull for date range: {start_date} to {end_date}")
#     
#     chat_records = get_chat_data(
#         date_from=start_date,
#         date_to=end_date,
#         timezone="America/New_York"
#     )
#     
#     if chat_records:
#         chat_df = process_chat_data(chat_records)
#         save_to_silver_table(chat_df, "silver.dechats")
#         
#         messages_df = create_messages_df(chat_df)
#         if messages_df:
#             save_to_silver_table(messages_df, "silver.dechats_messages")
#     
#     return len(chat_records)

# # Example usage:
# # records_pulled = manual_date_pull('2024-01-01', '2024-01-31')
# # print(f"Pulled {records_pulled} chat records")