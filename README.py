# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Instructions: 
# MAGIC - To run the ETL for Kingston data, go to Workflows -> Kingston ETL -> Run now. 
# MAGIC - For a new city, you will need to clone this folder and give it a new name (ex: alchemer_baltimore_etl).
# MAGIC   - Then, go to Workflows - > Create Job
# MAGIC       - There will be 5 tasks to create. These tasks will comprise of: extract, transform, roll_up, merge_census,write_to_volume
# MAGIC       - Within each task, for the "Path" field, you can click on the new folder/notebook location where the extract, transform, roll_up, merge_census, and write_to_volume notebooks are stored
# MAGIC       - Once you are done creating the 5 tasks, you should see a visual flow chart of the pipeline and be able to run the workflow
# MAGIC       - Note: you can also change the compute cluster for the workflow to run on and schedule runs (hourly, daily, etc.)
# MAGIC
# MAGIC # File Structure:
# MAGIC - utils: notebook that contains all constants and functions that are referenced during the ETL process
# MAGIC - extract_alchemer_data: notebook that contains code to extract Alchemer survey data from the Google Sheets URL and write the raw data to the bronze schema
# MAGIC - transform_alchemer_data: notebook that contains code to perform transformations and write transformed survey data to the silver schema
# MAGIC - rollup_alchemer_data: notebook that contains code to perform rollups and write tables to the gold schema
# MAGIC - merge_census: notebook that merges rollup data with census demographic and performs additional representation calculations
# MAGIC - write_to_volume: writes all tables in the gold schema to the Volume as CSV and JSON formats