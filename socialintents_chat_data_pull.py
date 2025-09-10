# Databricks notebook source
# MAGIC %md
# MAGIC # SocialIntents Chat Data Daily Pull
# MAGIC 
# MAGIC This notebook pulls chat histories from the SocialIntents REST API on a daily basis.
# MAGIC 
# MAGIC **API Details:**
# MAGIC - Base URL: https://api.socialintents.com/v1/api/
# MAGIC - Authentication: HTTP Basic Auth
# MAGIC - Account ID: 2c9fa36b72b45bda0172c33055963258
# MAGIC - REST API Token: c2a8fba7-d05a-464a-b7e8-8a77005c7c5d
# MAGIC 
# MAGIC **Schedule:** Run daily to pull previous day's chat data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Configuration

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
# MAGIC ## 2. API Configuration and Authentication

# COMMAND ----------

<function_calls>
<invoke name="TodoWrite">
<parameter name="todos">[{"content": "Create Databricks notebook for SocialIntents API integration", "status": "completed", "activeForm": "Creating Databricks notebook for SocialIntents API integration"}, {"content": "Set up authentication and API configuration", "status": "in_progress", "activeForm": "Setting up authentication and API configuration"}, {"content": "Implement daily data pull logic with date parameters", "status": "pending", "activeForm": "Implementing daily data pull logic with date parameters"}, {"content": "Add data transformation and schema definition", "status": "pending", "activeForm": "Adding data transformation and schema definition"}, {"content": "Include error handling and logging", "status": "pending", "activeForm": "Including error handling and logging"}]