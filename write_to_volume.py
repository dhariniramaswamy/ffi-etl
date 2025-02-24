# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

# Retrieve all table names from the wiatt.gold schema
tables_df = spark.sql("SHOW TABLES IN wiatt.gold")
tables_list = [row.tableName for row in tables_df.collect() if city in row.tableName]

for table_name in tables_list:
    # Load each table as a DataFrame
    df = spark.read.table(f"wiatt.gold.{table_name}")
    write_df_to_volume_json(df, temp_dbfs_path, final_dbfs_path, table_name)
    write_df_to_volume_json(df, temp_volume_path, final_volume_path, table_name)
    pdf = df.toPandas()
    pdf.to_csv(f"{final_volume_path}/{table_name}.csv", index=False)