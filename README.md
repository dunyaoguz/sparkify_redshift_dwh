# sparkify_redshift_dwh

## Summary

This is a data engineering project aimed to get practice with AWS Redshift, ETL pipelines and IaC (infrastructure-as-code) techniques. It is composed of the following steps:

* Using IaC, build the cloud data warehouse infrastructure for Sparkify, an imaginary music streaming app. 
* Build a data pipeline that extracts raw data from S3 buckets and stages them in Redshift.
* Transform the extracted data into a set of dimensional tables for Sparkify's analytics team.

## Directory

1. `cluster_launcher.py`: Creates & deletes a Redshift cluster with 4 dc2.large type nodes and s3 read-only access.
2. `data_check.ipnyb`: Checks the data in the s3 buckets where the raw data resides. 
3. `example_data`: Includes example files from the raw data.
4. `log_path_json.json`: Specifies the order of the keys in the json files for raw log data.
5. `sql_queries.py`: Includes the SQL statements needed in creating and inserting data into Sparkifydb. 
6. `create_tables.py`: Creates the Sparkify STAR schema tables and the staging tables needed for data insertion in Redshift.
7. `etl.py`: Copies raw data from s3 buckets into staging tables, and inserts them into the Sparkifydb.
8. `example.env`: This is an example of how your .env file should look like if you want to clone and run this project yourself.

## Quick Start



## Example Queries

## Tech Stack
* AWS 
* boto3
* dotenv
* os
* pandas
* psycopg2
* json
