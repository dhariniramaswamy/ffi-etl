# Databricks notebook source
# DBTITLE 1,Run Utils Library
# MAGIC %run ./utils

# COMMAND ----------

# DBTITLE 1,Read in Census Demographic Data
pdf = pd.read_csv(census_url)
census_df = spark.createDataFrame(pdf)
census_df = census_df.withColumnsRenamed({"Demographic": "Census_Demographic", "Category": "Census_Category"})
display(census_df)

# COMMAND ----------

# DBTITLE 1,Read in Roll-Up Data
rollup_df = spark.sql(f"SELECT * FROM {roll_up_table_path}")
rollup_df = rollup_df.withColumnsRenamed({"Demographic": "Rollup_Demographic", "Category": "Rollup_Category"})
display(rollup_df)

# COMMAND ----------

# DBTITLE 1,Join Census & Roll-Up Data
new_rollup = census_df.join(rollup_df, [census_df.Census_Demographic == rollup_df.Rollup_Demographic, census_df.Census_Category == rollup_df.Rollup_Category], how="outer")
display(new_rollup)

# COMMAND ----------

# defines window specification that groups rows with the same 'Census_Demographic' value
windowSpec = Window.partitionBy("Census_Demographic").orderBy("Census_Demographic")

# if both the 'Rollup_Demographic' and 'Rollup_Category' columns are null, then set the 'Total Responses' column to the first non-null value of 'Total Responses' for that 'Census_Demographic' value
new_rollup = new_rollup.withColumn("Total Responses", 
                                   when(col("Rollup_Demographic").isNull() & col("Rollup_Category").isNull(), 
                                        first("Total Responses", ignorenulls=True).over(windowSpec))
                                   .otherwise(col("Total Responses"))) \
                         .withColumn("# of Survey Responses", when(col("Rollup_Demographic").isNull() & col("Rollup_Category").isNull(), 0).otherwise(col("# of Survey Responses"))) \
                       .withColumn("% of Survey Responses", when(col("Rollup_Demographic").isNull() & col("Rollup_Category").isNull(), lit(0)).otherwise(col("% of Survey Responses"))) \
                        .withColumn("Rollup_Demographic", when(col("Rollup_Demographic").isNull(), col("Census_Demographic")).otherwise(col("Rollup_Demographic"))) \
                       .withColumn("Rollup_Category", when(col("Rollup_Category").isNull(), col("Census_Category")).otherwise(col("Rollup_Category")))

display(new_rollup)

# COMMAND ----------

# handles missing values in the Census%, Census_Demographic, and Census_Category columns
new_rollup = new_rollup.withColumn("Census %", 
                                   when(col("Census_Demographic").isNull() & col("Census_Category").isNull(), None)
                                   .otherwise(col("Census %"))) \
                        .withColumn("Census_Demographic", 
                                    when(col("Census_Demographic").isNull(), col("Rollup_Demographic"))
                                    .otherwise(col("Census_Demographic"))) \
                        .withColumn("Census_Category", 
                                    when(col("Census_Category").isNull(), col("Rollup_Category"))
                                    .otherwise(col("Census_Category")))
display(new_rollup)

# COMMAND ----------

# updates # of Survey Responses and % of Survey Responses columns only when there is non-null Census information and the corresponding rollup information is null
new_rollup = new_rollup.withColumn("# of Survey Responses", 
                                   when(col("Census_Demographic").isNotNull() & col("Census_Category").isNotNull() & col("Census %").isNotNull() & col("Rollup_Demographic").isNull() & col("Rollup_Category").isNull(), 
                                        0)
                                   .otherwise(col("# of Survey Responses")))

new_rollup = new_rollup.withColumn("% of Survey Responses", 
                                   when(col("Census_Demographic").isNotNull() & col("Census_Category").isNotNull() & col("Census %").isNotNull() & col("Rollup_Demographic").isNull() & col("Rollup_Category").isNull(), 
                                        "0 %")
                                   .otherwise(col("% of Survey Responses")))

# COMMAND ----------

# consolidate Demographic and Category columns
new_rollup = new_rollup.withColumnsRenamed({"Census_Demographic": "Demographic", "Census_Category": "Category"}) \
                        .drop("Rollup_Demographic", "Rollup_Category")
display(new_rollup)

# COMMAND ----------

new_rollup = new_rollup.withColumn("Census %", regexp_replace("Census %", "%", "").cast("float")) \
                       .withColumn("% of Survey Responses", regexp_replace("% of Survey Responses", "%", "").cast("float"))
display(new_rollup)

# COMMAND ----------

# calculate '% Difference' column
new_rollup = new_rollup.withColumn("% Difference", 
                                   when(col("Census %").isNotNull() & col("% of Survey Responses").isNotNull(), 
                                        col("% of Survey Responses") - col("Census %"))
                                   .otherwise(None))
display(new_rollup)

# COMMAND ----------

# create 'Representation Status' column based on conditions
new_rollup = new_rollup.withColumn("Representation Status", 
                                   expr(f"""
                                   CASE 
                                     WHEN `% Difference` < {representation_negative_constant} THEN 'Under-Represented'
                                     WHEN `% Difference` > {representation_positive_constant} THEN 'Over-Represented'
                                     WHEN `% Difference` > {representation_negative_constant} AND `% Difference` < {representation_positive_constant} THEN 'Looks Good'
                                     ELSE NULL
                                   END
                                   """))
display(new_rollup)

# COMMAND ----------

# calculate 'Additional Responses Needed' column
new_rollup = new_rollup.withColumn("Additional Responses Needed", 
                                   when((col("Representation Status") == "Under-Represented") | 
                                        (col("Representation Status") == "Over-Represented"), 
                                        round(abs(col("% Difference") * col("Total Responses") / 100)))
                                   .otherwise(None))
new_rollup = new_rollup.withColumn("Additional Responses Needed", round(round(col("Additional Responses Needed") / 10) * 10))
display(new_rollup)

# COMMAND ----------

# round and format values
new_rollup = new_rollup.withColumn("Census %", concat(round(col("Census %"), 1), lit("%"))) \
                       .withColumn("% of Survey Responses", concat(round(col("% of Survey Responses"), 1), lit("%"))) \
                       .withColumn("% Difference", concat(round(col("% Difference"), 1), lit("%")))
new_rollup = new_rollup.withColumnRenamed("Census %", "% of Population (Census)")
display(new_rollup)

# COMMAND ----------

# Get the max date from the specified table
max_date_submitted = spark.table("wiatt.silver.kingston_survey_data_transformed") \
                          .select(max("Survey Date Submitted").alias("max_date")) \
                          .collect()[0]["max_date"]

# Format the date as DD/MM/YYYY
formatted_max_date = date_format(lit(max_date_submitted), "MM/dd/yyyy")

# Add the "Data Last Updated" column with the formatted max date
new_rollup = new_rollup.withColumn("Data Last Updated", lit(formatted_max_date))

display(new_rollup)

# COMMAND ----------

# Define window specification for ordering within each Demographic group
windowSpec = Window.partitionBy("Demographic").orderBy(col("Display Order").asc_nulls_last(), col("Category").asc())

# Apply the window specification with row_number to maintain the order
new_rollup = new_rollup.withColumn("row_num", row_number().over(windowSpec))

# Display the DataFrame, grouped by Demographic and ordered as specified
new_rollup = new_rollup.orderBy("Demographic", "row_num")

# Drop the temporary row_num column
new_rollup = new_rollup.drop("row_num", "Display Order")
display(new_rollup)

# COMMAND ----------

write_to_delta(new_rollup, roll_up_table_path, mode="overwrite")