# Databricks notebook source
# DBTITLE 1,Run Utils Notebook
# MAGIC %run ./utils

# COMMAND ----------

# DBTITLE 1,Initialize Constants
# read in silver table and store in a Spark dataframe
df_silver = spark.table(silver_table_path)

# initialize list containing all dfs to write to delta table
dfs_to_write = []

# COMMAND ----------

# DBTITLE 1,Filter for Valids
all_valid_survey = df_silver.filter(col("Is_Invalid") == "Valid")
dfs_to_write.append((all_valid_survey, all_valid_survey_table_path))

# COMMAND ----------

# DBTITLE 1,Filter for Invalids
invalid_survey = df_silver.filter(col("Is_Invalid") != "Valid")
dfs_to_write.append((invalid_survey, invalid_survey_table_path))

# COMMAND ----------

# DBTITLE 1,Demographic Roll-Up
# create list of dataframes and apply process_demographic function to demographic columns
result_dfs = [process_demographic(all_valid_survey, col, name) for col, name in demographics]

# extract first two dataframes from the results_dfs list
df1 = result_dfs[0]
df2 = result_dfs[1]

# combines all dataframes into one dataframe by performing a union operation
unioned_dfs = reduce(lambda df1, df2: df1.union(df2), result_dfs)

# columns to include in the final roll_up
final_columns = ["Demographic", "Category", "# of Survey Responses", "% of Survey Responses", "Total Responses"]
roll_up = unioned_dfs.select(final_columns)
dfs_to_write.append((roll_up, roll_up_table_path))

# COMMAND ----------

# DBTITLE 1,Write to Delta Table
for df, table_path in dfs_to_write:
    write_to_delta(df, table_path, mode="overwrite")

# COMMAND ----------

display(all_valid_survey)

# COMMAND ----------

display(invalid_survey)