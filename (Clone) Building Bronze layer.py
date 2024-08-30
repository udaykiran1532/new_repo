# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists timmala_kiran_databricks_npmentorskool_onmicrosoft_com;

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists timmala_kiran_databricks_npmentorskool_onmicrosoft_com.test_ecom;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog timmala_kiran_databricks_npmentorskool_onmicrosoft_com;
# MAGIC use schema test_ecom;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE customers_into (
# MAGIC     CustomerID VARCHAR(20) PRIMARY KEY,
# MAGIC     FirstName VARCHAR(50),
# MAGIC     LastName VARCHAR(50),
# MAGIC     Email VARCHAR(100),
# MAGIC     PhoneNumber VARCHAR(15),
# MAGIC     DateOfBirth DATE,
# MAGIC     RegistrationDate DATE,
# MAGIC     PreferredPaymentMethodID VARCHAR(20)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO customers_into(CustomerID, FirstName, LastName, Email, PhoneNumber, DateOfBirth, RegistrationDate, PreferredPaymentMethodID)
# MAGIC VALUES 
# MAGIC ('CUST-15001', 'Vardaniya', 'Acharya', 'vardaniya.acharya274@icloud.com', '918319E11', '1993-12-19', '2012-02-05', 'PM-002'),
# MAGIC ('CUST-15002', 'Zaina', 'Dâ€™Alia', 'zaina282@yahoo.com', '919461E11', '1981-02-17', '2021-12-06', 'PM-002'),
# MAGIC ('CUST-15003', 'Zaina', 'Din', 'dinzaina590@gmail.com', '919334E11', '1995-11-03', '2014-12-11', 'PM-001'),
# MAGIC ('CUST-15004', 'Amani', 'Bhattacharyya', 'amani.144@yahoo.com', '918052E11', '1991-10-22', '2017-07-20', 'PM-004'),
# MAGIC ('CUST-15005', 'Aayush', 'Grover', 'aayush.742@outlook.com', '919895E11', '1975-12-26', '2018-12-11', 'PM-001'),
# MAGIC ('CUST-15006', 'Charvi', 'Varty', 'charvi_728@yahoo.com', '919820E11', '2000-01-25', '2023-07-04', 'PM-002');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE customers_overwrite_test (
# MAGIC     CustomerID VARCHAR(20) PRIMARY KEY,
# MAGIC     FirstName VARCHAR(50),
# MAGIC     LastName VARCHAR(50),
# MAGIC     Email VARCHAR(100),
# MAGIC     PhoneNumber VARCHAR(15),
# MAGIC     DateOfBirth DATE,
# MAGIC     RegistrationDate DATE,
# MAGIC     PreferredPaymentMethodID VARCHAR(20)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE customers_overwrite_test
# MAGIC VALUES 
# MAGIC ('CUST-15001', 'Vardaniya', 'Acharya', 'vardaniya.acharya274@icloud.com', '918319E11', '1993-12-19', '2012-02-05', 'PM-002'),
# MAGIC ('CUST-15002', 'Zaina', 'Dâ€™Alia', 'zaina282@yahoo.com', '919461E11', '1981-02-17', '2021-12-06', 'PM-002'),
# MAGIC ('CUST-15003', 'Zaina', 'Din', 'dinzaina590@gmail.com', '919334E11', '1995-11-03', '2014-12-11', 'PM-001'),
# MAGIC ('CUST-15004', 'Amani', 'Bhattacharyya', 'amani.144@yahoo.com', '918052E11', '1991-10-22', '2017-07-20', 'PM-004');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers_into

# COMMAND ----------

display(dbutils.fs.ls("/mnt/test-bronze-layer/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists customers_copy_into;

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO customers_copy_into
# MAGIC FROM '/mnt/test-bronze-layer/'
# MAGIC FILEFORMAT = CSV 
# MAGIC FORMAT_OPTIONS ('header'='true','inferSchema'='true', 'mergeSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

def clean_and_save_csv(mount_name, table_name):
    # Corrected the path format by adding a forward slash after dbfs:
    df = spark.read.option("header", "true").csv(f"dbfs:/mnt/{mount_name}/{table_name}.csv")
    print("Original Columns: ", df.columns)
    cleaned_columns = [re.sub(r'[^a-zA-Z0-9]+', '_', col).strip('_') for col in df.columns]

    for original, cleaned in zip(df.columns, cleaned_columns):
        df = df.withColumnRenamed(original, cleaned)
    print("Cleaned columns:", df.columns)

    # Corrected the path format by adding a forward slash after dbfs: for the temp_path as well
    temp_path = f"dbfs:/{mount_name}/temp_cleaned_{table_name}/"
    df.write.option("header", "true").csv(temp_path, mode="overwrite")

    print(f"Cleaned data written to {temp_path}")

    return temp_path


for table in Tables:
    clean_and_save_csv("test_ecom", table)

# COMMAND ----------

def load_to_delta_table(temp_path, delta_table_name):
    # Step 8: Create the Delta table if it does not exist
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {delta_table_name} USING DELTA
    """
    spark.sql(create_table_query)
    print(f"Delta table {delta_table_name} created or already exists.")
    
    # Step 9: Use COPY INTO to load data from the temporary location to the Delta table
    copy_into_query = f"""
    COPY INTO {delta_table_name}
    FROM '{temp_path}'
    FILEFORMAT = CSV
    FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
    """
    spark.sql(copy_into_query)

    print(f"Data loaded into {delta_table_name} from {temp_path}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers_copy_into;

# COMMAND ----------


