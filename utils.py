# Databricks notebook source
# DBTITLE 1,Import Libraries
import pandas as pd
from pyspark.sql.functions import col, to_timestamp, expr, unix_timestamp, udf, lit, round, when, split, trim, upper, concat, lower, regexp_replace, count, format_string, sum, abs, round, coalesce, max, date_format, replace, row_number
from pyspark.sql.types import IntegerType, StringType
from functools import reduce
from pyspark.sql.window import Window
from pyspark.sql.functions import first
import os

# COMMAND ----------

# DBTITLE 1,Define Variables
## CAN EDIT: Google Sheets url to extract survey data from
online_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRT6qI-sRWVyUxV2sLa_3LwecHZU8eZBv4cIr8FHcSrkR_qfH-AKMssgF0m0ekUYvdS3FQz9tvAyEjp/pub?gid=0&single=true&output=csv"
offline_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRZgMPur8dJegzZED2tQPHZUKQyJugPWobAB6gCn90aSKIvlEaNaJklFN-3F0tH1RhtHtzqmmO5Uebq/pub?gid=0&single=true&output=csv"
census_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vT_5bM9CipB6IZBY9aqd6VwMgPXFq2N2WlM6_rxrngjCbpyGgQ5VkkudeT0_IA_W_Jhv4VcM9UJyhrL/pub?gid=1590178794&single=true&output=csv"

# CAN EDIT for a different city
city = "kingston" 

# CAN EDIT: table variables
catalog = "wiatt"
bronze_table_path = f"{catalog}.bronze.{city}_survey_data_raw"
silver_table_path = f"{catalog}.silver.{city}_survey_data_transformed"
all_valid_survey_table_path = f"{catalog}.gold.{city}_all_valid_survey"
invalid_survey_table_path = f"{catalog}.gold.{city}_invalid_survey"
roll_up_table_path = f"{catalog}.gold.{city}_representation_check"

# CAN EDIT: constant used to calculate Representation Status in final rollup
representation_negative_constant = -5
representation_positive_constant = 5

# storage paths
temp_volume_path = f"/Volumes/wiatt/gold/ev_gold/raw_outputs_{city}"
final_volume_path = f"/Volumes/wiatt/gold/ev_gold/outputs_{city}"
temp_dbfs_path = f"/dbfs/FileStore/raw_outputs_{city}"
final_dbfs_path = f"/dbfs/FileStore/outputs_{city}"

# COMMAND ----------

# read in Google sheets containing column renaming and deletions
standard_questions_map = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vTyp4j_IntF-Gm1bfB-9CELfdDHZycPBjkrv6iT832Lsp6Ub6o8Mnlx-Iu_RETyEZwN2-6FdfP-eVCZ/pub?gid=1808210837&single=true&output=csv")
kingston_cols_to_rename = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vTyp4j_IntF-Gm1bfB-9CELfdDHZycPBjkrv6iT832Lsp6Ub6o8Mnlx-Iu_RETyEZwN2-6FdfP-eVCZ/pub?gid=871371130&single=true&output=csv")
kingston_cols_to_delete = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vTyp4j_IntF-Gm1bfB-9CELfdDHZycPBjkrv6iT832Lsp6Ub6o8Mnlx-Iu_RETyEZwN2-6FdfP-eVCZ/pub?gid=1919452986&single=true&output=csv")
open_text_fields_df = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vTyp4j_IntF-Gm1bfB-9CELfdDHZycPBjkrv6iT832Lsp6Ub6o8Mnlx-Iu_RETyEZwN2-6FdfP-eVCZ/pub?gid=2036538749&single=true&output=csv")

# COMMAND ----------

# consolidate all column renaming values into a dictionary
standard_questions_map_dict = standard_questions_map.set_index('column_in_kingston_csv')['standard_column_name'].to_dict()
custom_cols_dict = kingston_cols_to_rename.set_index('column_in_kingston_csv')["rename_to"].to_dict()
cols_to_rename_dict = standard_questions_map_dict | custom_cols_dict

# COMMAND ----------

# DBTITLE 1,Columns to Drop
## convert list of columns to delete into a list
cols_to_delete = kingston_cols_to_delete["cols_delete"].tolist()

# COMMAND ----------

# DBTITLE 1,Likert Columns
# CAN EDIT: Likert questions map to domains of wellbeing
likert_columns_dict = {
    "5DW Score: Safety": ["Safety: Impact my safety"],
    "5DW Score: Relevant Resources": [
        "Resources: Information and opportunities",
        "Resources: Food, sleep, housing",
        "Resources: Ability to pay my bills",
        "Resources: Ability to have fun"
    ],
    "5DW Score: Mastery": [
        "Mastery: Skill and confidence",
        "Mastery: Control and choice",
        "Mastery: Rights are protected"
    ],
    "5DW Score: Social Connectedness": [
        "Social: Feeling I belong here",
        "Social: Connect with people",
        "Social: Take care of people",
        "Social: Knowledge that I matter"
    ],
    "5DW Score: Stability": [
        "Stability: Stick to my routines",
        "Stability: Things are about to fall apart",
        "Stability: Deal with life hassles"
    ]
}

likert_columns = [item for sublist in likert_columns_dict.values() for item in sublist]

# COMMAND ----------

# DBTITLE 1,Open Text Fields
# convert open text response questions pandas dataframe into a list
open_text_fields_lst = open_text_fields_df["open_text_columns"].tolist()

# COMMAND ----------

# DBTITLE 1,Null Phrases
# CAN EDIT: list of responses that can qualify as a null response
null_phrases = [
    "na", "n/a", "not applicable", "no response", "does not apply", "no thank you", "no thanks",
    "no comment", "it dont", "none", "i have no idea", "no idea", "no time", "no realmente", "nada mas que agregar"
    "not really", "nothing", "-", "", "i not to say", "nope", "no", "no I do not", "nothings", "nan"]

# COMMAND ----------

# demographic columns to group by for roll-up
# CAN EDIT, but must follow list of tuples format
demographics = [
    ("Gender", "Gender"),
    ("Age", "Age"),
    ("Race/Ethnicity", "Race/Ethnicity"),
    ("Household Income", "Household Income"),
    ("Survey Language", "Language"),
    ("CM Name", "CM Name")
]

# COMMAND ----------

# DBTITLE 1,Helper Functions
def write_to_delta(df, path_to_table, mode):
    '''
    Writes a dataframe to a delta table.

    Inputs:
        - df (Spark dataframe): dataframe to write
        - path_to_table (string): path to wrote to. 
            - Follows the format 'catalog.schema.table_name'
        - mode (string): overwrite or append

    Returns:
        None
    '''
 
    try:
        df.write.mode(mode).option("delta.columnMapping.mode", "name").option("overwriteSchema", "true").saveAsTable(path_to_table)
        print(f"Successfully performed {mode} operation of {path_to_table}")
    except Exception as e:
        print(e)

    
# Replace special characters in values
def replace_special_chars(value):
    if value and isinstance(value, str):
        value = value.replace("â€™", "'")
        ## may need to keep certain special chars as is or edit at the survey level
        # ex: Porque es muy importante para nosotros quÃ© vivimos acÃ¡
        value = value.replace("Ã", "i")
    return value
    

# user-defined function (udf) to apply replace_special_chars to all column values
replace_special_chars_udf = udf(replace_special_chars, StringType())


def map_likert(response):
    '''
    Maps likert responses to numeric values.

    Inputs:
        - response (string): response given for likert question
    
    Returns:
        - int: numeric value for response
    '''

    mapping = {
        "No change": 0,
        "A little better": 1,
        "A lot better": 2,
        "A little worse": -1,
        "A lot worse": -2
    }
    return mapping.get(response, 0)


# user-defined function (udf) to apply the map_likert function for an entire column
map_likert_udf = udf(map_likert, IntegerType())


def replace_with_null(column):
    return when(
        trim((regexp_replace(lower(col(column)), r'[^\w\s]', ''))).isin(null_phrases),None
    ).otherwise(col(column))


def process_demographic(df, demographic_col, demographic_name):
    '''
    Process demographic data to get counts, percentages, and other relevant information.
    
    Inputs:
        - df (Spark dataframe): input dataframe
        - demographic_col (string): column name of the demographic to process
        - demographic_name: name of the demographic (e.g., "Gender", "Age", etc.)

    Returns:
        - demographic_group (Spark dataframe): dataframe with demographic roll-ups
    '''
    
    # Group by demographic column and count
    demographic_group = df.groupBy(demographic_col).count()
    
    # Calculate total responses
    total_responses = demographic_group.select(sum("count").alias("total")).collect()[0]["total"]
    
    # Calculate total responses excluding "Unknown"
    total_responses_excl_unknown = demographic_group.filter(col(demographic_col) != "Unknown") \
                                                     .select(sum("count").alias("total")).collect()[0]["total"]
    
    # Add columns: "Category", "Demographic", "Total Responses", and "'%' of Survey Responses"
    demographic_group = demographic_group.withColumn("Demographic", lit(demographic_name)) \
        .withColumnRenamed(demographic_col, "Category") \
        .withColumn("Total Responses", when(col("Category") != "Unknown", lit(total_responses_excl_unknown)).otherwise(lit(""))) \
        .withColumn("% of Survey Responses", col("count") / total_responses_excl_unknown * 100) \
        .withColumn("% of Survey Responses", format_string("%.2f%%", col("% of Survey Responses"))) \
        .withColumnRenamed("count", "# of Survey Responses")

    demographic_group = demographic_group.orderBy(col("Category").desc())
    
    return demographic_group


def cp_file_temp(temp_path, final_path, file_name):
    temp_file_names = dbutils.fs.ls(f"{temp_path}")
    name = ''
    for temp_filename in temp_file_names:
        if temp_filename.name.endswith(".json"):
            temp_name = temp_filename.name
    try:
        dbutils.fs.cp(f"{temp_path}/" + temp_name, f"{final_path}/{file_name}.json")
        print(f"Success: Copied {temp_path}/{temp_name} to {final_path}/{file_name}.json")
    except Exception as e:
        print(e)

    try:
        dbutils.fs.rm(f"{temp_path}", recurse=True)
        print(f"Success: Deleted {temp_path}")
    except Exception as e:
        print(e)


def write_df_to_volume_json(df, temp_path, final_path, file_name):
    try:
        df.coalesce(1).write.mode("overwrite").json(f"{temp_path}")
        print(f"Success: Wrote {file_name}.json as a temporary file to {temp_path}")
    except Exception as e:
        print(e)

    cp_file_temp(temp_path, final_path,file_name)