# Databricks notebook source
# MAGIC %md
# MAGIC 1. setup your databricks environment
# MAGIC 2. create an app registration and generate the client_secret
# MAGIC 3. in databricks workspace add the identity - Microsoft Entra application ID and Service Principal Name
# MAGIC 4. create all your secrets in databricks workspace using the databricks cli
# MAGIC 5. add the IAM privleges for the service principal to the storage account
# MAGIC   - Storage Queue Data Contributor
# MAGIC   - Storage Blob Data Owner
# MAGIC 6. create the containers in the storage account and path
# MAGIC   - container: adbquickstart
# MAGIC   - path in container: adbdev/ingest/drop/
# MAGIC 7. create the queue in the storage account
# MAGIC 8. Setup an event for "blob created" to the storage queue
# MAGIC 9. Upload some CSV files -- see the example CSV below
# MAGIC
# MAGIC
# MAGIC >NOTE: must run from the databricks cli
# MAGIC ```shell
# MAGIC  databricks secrets create-scope adbdev
# MAGIC  databricks secrets put-secret adbdev tenant_id
# MAGIC  databricks secrets put-secret adbdev subscription_id
# MAGIC  databricks secrets put-secret adbdev client_id
# MAGIC  databricks secrets put-secret adbdev client_secret
# MAGIC  databricks secrets put-secret adbdev storage_account_name
# MAGIC  ```
# MAGIC
# MAGIC  Sample CSV
# MAGIC
# MAGIC  ```csv
# MAGIC siteId,date,dayPartId,startTime,endTime,serviceTime,orderId,netSaleAmount
# MAGIC "1",2024-04-15 12:15:00.000-04:00,1,2024-04-15 12:15:00.000-04:00,2024-04-15 12:17:05.000-04:00,125,"527150",10
# MAGIC "1",2024-04-15 12:15:30.000-04:00,1,2024-04-15 12:15:30.000-04:00,2024-04-15 12:17:23.000-04:00,113,"918670",10
# MAGIC "1",2024-04-15 12:16:00.000-04:00,1,2024-04-15 12:16:00.000-04:00,2024-04-15 12:17:57.000-04:00,117,"284980",10
# MAGIC "1",2024-04-15 12:16:30.000-04:00,1,2024-04-15 12:16:30.000-04:00,2024-04-15 12:18:29.000-04:00,119,"191422",10
# MAGIC "1",2024-04-15 12:17:00.000-04:00,1,2024-04-15 12:17:00.000-04:00,2024-04-15 12:18:44.000-04:00,104,"581514",10
# MAGIC "1",2024-04-15 12:17:30.000-04:00,1,2024-04-15 12:17:30.000-04:00,2024-04-15 12:19:34.000-04:00,124,"937164",10
# MAGIC "1",2024-04-15 12:18:00.000-04:00,1,2024-04-15 12:18:00.000-04:00,2024-04-15 12:20:02.000-04:00,122,"701144",10
# MAGIC "1",2024-04-15 12:18:30.000-04:00,1,2024-04-15 12:18:30.000-04:00,2024-04-15 12:20:20.000-04:00,110,"740754",10
# MAGIC "1",2024-04-15 12:19:00.000-04:00,1,2024-04-15 12:19:00.000-04:00,2024-04-15 12:20:37.000-04:00,97,"564473",10
# MAGIC "1",2024-04-15 12:19:30.000-04:00,1,2024-04-15 12:19:30.000-04:00,2024-04-15 12:21:41.000-04:00,131,"813660",10
# MAGIC
# MAGIC  ```

# COMMAND ----------

# MAGIC %md
# MAGIC # You Must first run the next two code cells----

# COMMAND ----------

from pyspark.sql.functions import input_file_name, current_timestamp

secret_scope = "adbdev"

tenant_id = dbutils.secrets.get(scope=secret_scope, key="tenant_id")
subscription_id = dbutils.secrets.get(scope=secret_scope, key="subscription_id" )
client_id = dbutils.secrets.get(scope=secret_scope, key="client_id")
client_secret = dbutils.secrets.get(scope=secret_scope, key="client_secret")
# Define the input path for the files
container = "adbquickstart"
storage_account_name = dbutils.secrets.get(scope=secret_scope, key="storage_account_name")
directory = "adbdev/ingest/drop"

input_path = "abfss://%s@%s.dfs.core.windows.net/%s" % (container, storage_account_name, directory)

print("source location: %s", input_path)

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", 
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

file_path = f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/{directory}/events.csv"
drop_path = f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/{directory}/"

print(file_path)

# Now you can read data from Azure Storage with abfss protocol
df = spark.read.csv(f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/{directory}/events.csv")
# display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## This is a storage queue based trigger

# COMMAND ----------

# for this section -- clear the queue first ---

# Import functions
from pyspark.sql.functions import col, current_timestamp

# Define variables used in code below
# file_path = "/databricks-datasets/structured-streaming/events"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"{username}_etl_queue_quickstart"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_queue_quickstart"
print(checkpoint_path)

# Clear out data from previous demo execution
# comment out these two lines to allow continous append
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)

(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .option("cloudFiles.useNotifications", "true")  # Use notification-based triggering
  .option("cloudFiles.queueName", "newfile")  # Event Grid queue for notifications
  .option("cloudFiles.subscriptionId", subscription_id)
  .option("cloudFiles.tenantId", tenant_id)
  .option("cloudFiles.clientId", client_id)
  .option("cloudFiles.clientSecret", client_secret)
  .load(drop_path)
  .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(table_name))



# COMMAND ----------

# MAGIC %md
# MAGIC ## Run this cell for a "file listing" based trigger....

# COMMAND ----------

# Import functions
from pyspark.sql.functions import col, current_timestamp

# Define variables used in code below
# file_path = "/databricks-datasets/structured-streaming/events"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"{username}_etl_quickstart"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"
print(checkpoint_path)

# Clear out data from previous demo execution
# comment out these two lines to allow continous append
# spark.sql(f"DROP TABLE IF EXISTS {table_name}")
# dbutils.fs.rm(checkpoint_path, True)

# Configure Auto Loader to ingest CSV data to a Delta table
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(drop_path)
  .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(table_name))
