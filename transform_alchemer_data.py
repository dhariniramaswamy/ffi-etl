# Databricks notebook source
# DBTITLE 1,Run Utils Notebook
# MAGIC
# MAGIC %run ./utils

# COMMAND ----------

df_bronze = spark.table(bronze_table_path)

# COMMAND ----------

# format column headers and values
col_format = {col_name: replace_special_chars(col_name) for col_name in df_bronze.columns}
df_silver = df_bronze.withColumnsRenamed(col_format)


# COMMAND ----------

# replace special characters across all string columns in the DataFrame
df_silver = df_silver.select([
    regexp_replace(regexp_replace(col(c), 'â€™', "'"),'Ã', 'i').alias(c) if df_silver.schema[c].dataType == StringType() else col(c)
    for c in df_silver.columns
])

# COMMAND ----------

# replace null responses with 'null" in open text response questions
for col_name in open_text_fields_lst:
    try:
        df_silver = df_silver.withColumn(col_name, replace_with_null(col_name))
    except Exception as e:
        print("col_error: ", col_name)
        print(e)

# COMMAND ----------

# replace 'nan' values with null
for column in df_silver.columns:
    df_silver = df_silver.withColumn(column, when(col(column) == "nan", None).otherwise(col(column)))

# COMMAND ----------

# need to use PySpark's legacy time parser for calculating the Time Difference in subsequent block
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# cast the 'Time Started' and 'Survey Date Submitted" columns to a timestamp
df_silver = df_silver.withColumn("Time Started", to_timestamp(col("Time Started"), "MM/dd/yyyy h:mm:ss a")) \
                    .withColumn("Survey Date Submitted", to_timestamp(col("Survey Date Submitted"), "MM/dd/yyyy h:mm:ss a"))

# calculate the time difference between the 'Time Started' and 'Survey Date Submitted' columns
df_silver = df_silver.withColumn("Time Difference", expr("unix_timestamp(`Survey Date Submitted`) - unix_timestamp(`Time Started`)"))

# convert the Time Difference column to minutes
df_silver = df_silver.withColumn("Time Difference (Minutes)", round(col("Time Difference") / 60))


# COMMAND ----------

# map likert questions to numeric values and apply it to the dataframe
likert_dict_udf = {column: map_likert_udf(column) for column in likert_columns}
df_silver = df_silver.withColumns(likert_dict_udf)


# calculate averages for each new column and add them to the DataFrame
for new_col, old_cols in likert_columns_dict.items():
    quoted_old_cols = [f"`{col}`" for col in old_cols]
    df_silver = df_silver.withColumn(new_col, expr(f"round(("+ " + ".join(quoted_old_cols) + f") / {len(old_cols)}, 2)"))


# calculate a new column that is the average of all likert columns
columns_expr = [col(c) for c in likert_columns]
everything_expr = reduce(lambda a, b: a + b, columns_expr) / lit(len(likert_columns))
df_silver = df_silver.withColumn("5DW Score: Everything", round(everything_expr, 2))

# COMMAND ----------

# modify the Race/Ethnicity column according to requirements
df_silver = df_silver.withColumn(
    "Race/Ethnicity",
    when(
        col("Hispanic or Latinx") == "Yes", "Hispanic or Latinx"
    ).otherwise(
        when(col("Race/Ethnicity") == "Some other race (please write it in here)", "Other race")
        .when(col("Race/Ethnicity") == "I prefer not to answer this question", "Unknown")
        .when(col("Race/Ethnicity").isNull(), "Unknown")
        .otherwise(col("Race/Ethnicity"))
    )
)

# COMMAND ----------

# modify the Gender column according to requirements
df_silver = df_silver.withColumn(
    "Gender",
    when(
        (col("Gender").isNull()) | (col("Gender") == "Prefer not to say"), "Unknown"
    ).when(
        col("Gender") == "Write In", "Other"
    ).otherwise(
        col("Gender")
    )
)


# COMMAND ----------

# replace CM column blanks with 'Unknown'
# df_silver = df_silver.withColumn("CM Name", when(col("CM Name").isNull(), "Unknown").otherwise(col("CM Name")))

df_silver = df_silver.withColumn(
    "CM Name",
    when(
        (col("CM Name").isNull()) |
        (trim(col("CM Name")) == ""), 
        "Unknown"
    ).otherwise(col("CM Name"))
)

# COMMAND ----------

# replace blanks or nulls in 'Current living situation' column with 'Unknown'
df_silver = df_silver.withColumn(
    "Current living situation",
    when(
        (col("Current living situation").isNull()) |
        (trim(col("Current living situation")) == "") |
        (col("Current living situation") == "Prefer not to say"), 
        "Unknown"
    ).otherwise(col("Current living situation"))
)

# COMMAND ----------

# replace blanks or nulls in columns with 'Unknown', but these have 'I prefer not to answer this question' instead of 'Prefer not to say' as a response option
replace_vals_unknown = ['How many years lived in Kingston', 'Why are you interested in this project?', 'In a typical month, how difficult is it for your household to pay for usual household expenses?', 'Hispanic or Latinx']

for column in replace_vals_unknown:
    df_silver = df_silver.withColumn(
        column,
        when(
            (col(column).isNull()) |
            (trim(col(column)) == "") |
            (col(column) == "I prefer not to answer this question"), 
            "Unknown"
        ).otherwise(col(column))
        )

# COMMAND ----------

# replace zip code blanks or null values with 'Unknown'. otherwise, take the first 5 digits
df_silver = df_silver.withColumn(
    "IP Address - Zip Code",
    when(
        col("IP Address - Zip Code").isNull() |
        (col("IP Address - Zip Code").substr(1, 5).cast("int") == 0),
        "Unknown"
    ).otherwise(
        col("IP Address - Zip Code").substr(1, 5)
    )
)

# COMMAND ----------

# modify the Age column to a categorical column
df_silver = df_silver.withColumn(
    "Age",
    when(col("Age") <= 0, "Unknown")
    .when(col("Age") < 10, "Less than 10 years old")
    .when(col("Age") < 18, "10 to 17 years old")
    .when(col("Age") < 30, "18 to 29 years old")
    .when(col("Age") < 45, "30 to 44 years old")
    .when(col("Age") < 60, "45 to 59 years old")
    .when(col("Age") < 75, "60 to 74 years old")
    .when(col("Age") < 120, "75 years and older")
    .otherwise("Unknown")
)

# COMMAND ----------

# modify Household Income column to a categorical column
df_silver = df_silver.withColumn(
    "Household Income",
    when(
        col("Household Income").isNull() |
        (col("Household Income") == "I prefer not to answer this question"),
        "Unknown"
    ).when(
        (col("Household Income") == "Less than $20,000") |
        (col("Household Income") == "$20,000 to $49,999"),
        "Less than $50,000"
    ).otherwise(
        col("Household Income")
    )
)

# COMMAND ----------

# create Is_Invalid column based on survey completion conditions
df_silver = df_silver.withColumn(
    "Is_Invalid",
    when(
        (upper(col("Alchemer Admin Comments")) == "OK") | 
        (upper(col("Alchemer Admin Comments")) == "VALID"), 
        "Valid"
    ).when(
        col("Survey Completed?") == "Partial", 
        "Survey status is partially completed"
    ).when(
        col("Survey Completed?") == "Disqualified", 
        "Disqualified in Alchemer"
    ).when(
        col("Survey Completed?") != "Complete", 
        concat(lit("Survey status is "), col("Survey Completed?"))
    ).when(
        (col("Survey Link Used") == "Test link") | 
        (col("Survey Link Used") == "Test"), 
        "Survey was submitted via test link"
    ).when(
        col('IP Address - Country') != "United States", 
        "IP address outside USA"
    ).otherwise("Valid")
)

# COMMAND ----------

# drop columns
cols_to_delete.append("Time Difference")
df_silver = df_silver.drop(*cols_to_delete)

# COMMAND ----------

# DBTITLE 1,Write to Delta Table
write_to_delta(df_silver, silver_table_path, "overwrite")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM wiatt.silver.kingston_survey_data_transformed