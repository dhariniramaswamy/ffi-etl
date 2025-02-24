# Databricks notebook source
# DBTITLE 1,Run Utils Notebook
# MAGIC %run ./utils

# COMMAND ----------

# read in data from Google Sheets URL into Pandas dataframe
pdf_online = pd.read_csv(online_url)
pdf_offline = pd.read_csv(offline_url)

# COMMAND ----------

# add 'Response ID' in front of the Response ID values in the offline dataframe
pdf_offline["Response ID"] = "Offline #" + pdf_offline["Response ID"].astype(str)

# COMMAND ----------

# Convert the merge key columns in both DataFrames to string
merge_columns = list(pdf_online.columns.intersection(pdf_offline.columns))
pdf_online[merge_columns] = pdf_online[merge_columns].astype(str)
pdf_offline[merge_columns] = pdf_offline[merge_columns].astype(str)

# Perform an outer join on all of the columns to join online and offline data
pdf_outer_joined = pdf_online.merge(pdf_offline, on=merge_columns, how="outer")
display(pdf_outer_joined)

# COMMAND ----------

# strip leading and trailing spaces from column names
pdf_outer_joined.columns = [col.strip() for col in pdf_outer_joined.columns]

# COMMAND ----------

# error handling for column renaming
for col, col_rename in cols_to_rename_dict.items():
    if col not in pdf_outer_joined.columns:
        raise Exception(f"Column `{col}` not found in the dataframe")
        dbutils.notebook.exit("Column `{col}` not found in the dataframe")
    elif len(col_rename) > 150:
        raise Exception(f"Column rename `{col_rename}` is too long. Max length is 150 characters")
        dbutils.notebook.exit(f"Column rename `{col_rename}` is too long. Max length is 150 characters")

# COMMAND ----------

# rename columns to match PySpark naming constraints
pdf_outer_joined = pdf_outer_joined.rename(columns=cols_to_rename_dict)

# COMMAND ----------

df_bronze = spark.createDataFrame(pdf_outer_joined)

# COMMAND ----------

display(df_bronze)

# COMMAND ----------

write_to_delta(df_bronze, bronze_table_path, "overwrite")