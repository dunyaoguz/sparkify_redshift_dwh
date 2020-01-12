# sparkify_redshift_dwh

## Summary

This is a data engineering project aimed to get practice with AWS Redshift, ETL pipelines and IaC (infrastructure-as-code) techniques. It is composed of the following steps:

* Using IaC, build the cloud data warehouse infrastructure for Sparkify, an imaginary music streaming app. 
* Build a data pipeline that extracts raw data from S3 buckets and stages them in Redshift.
* Transform the extracted data into a set of dimensional tables for Sparkify's analytics team.

## Directory

1. `cluster_launcher.py`: Creates & deletes a Redshift cluster with 4 dc2.large type nodes and s3 read-only access.
2. `data_check.ipnyb`: Checks the data in the s3 buckets where the source raw data resides. 
3. `example_data`: Includes example files from the source raw data.
4. `log_path_json.json`: Specifies the order of the keys in the json files for raw log data.
5. `sql_queries.py`: Includes the SQL statements needed in creating and inserting data into Sparkifydb. 
6. `create_tables.py`: Creates the Sparkifydb star schema tables and the staging tables needed for data insertion.
7. `etl.py`: Copies raw data from s3 buckets into staging tables, and inserts them into the Sparkifydb.
8. `example.env`: This is an example of how your .env file should look like if you want to clone and run this project yourself. This is the file where your API keys and database passwords will reside. 

## Quick Start

1. Clone this repository.

``` 
git clone https://github.com/dunyaoguz/sparkify_redshift_dwh.git
cd sparkify_redshift_dwh
```

2. Install dependencies.

```
pip install boto3
pip install psycopg2
pip install psycopg2-binary
pip install python-dotenv
```

3. Create an AWS account if you don't already have one. Create an IAM user with admin access from AWS console. Download your credentials.
4. Add your AWS key and secret to your .env file, along with the master user name and password you want to use for your database.
5. Run cluster_launcher.py. 

```
python cluster_launcher.py
```

6. Copy the arn, host and port printed on your terminal, add it on your .env file. Wait until you see "cluster is available" printed out in terminal.
7. Run create_tables.py.

```
python create_tables.py
```

8. Run etl.py.

```
python etl.py
```

9. Congrats! You successfully created SparkifyDB. Now go run some fun queries on your data! You can do this from the Redshift console query editor. See the example queries section for inspiration.

10. Make sure to delete your Redshift cluster after at the end to avoid unnecessary costs. Remember, you'll be charged 1$ for each hour your cluster is live. To delete your Redshift cluster, go back to cluster_launcher.py, comment out line 105 (`create_cluster(ROLE_ARN)`) and uncomment lines 118 (`reset()`) and 119 (`check_status('deleted')`). Go back to terminal and run cluster_launcher.py. 

```
python cluster_launcher.py
```

Wait until "cluster is deleted" gets printed out in terminal.

## Schema



## Example Queries

Get the most streamed artists 

```
SELECT a.name
, COUNT(DISTINCT s.songplay_id) AS no_listens
FROM songplays AS s
LEFT JOIN artists AS a
ON s.artist_id = a.artist_id
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10
```

Get the most active users

```
SELECT CONCAT(u.firstname + '', u.lastname) AS user_name
, COUNT(DISTINCT s.songplay_id) AS no_listens
FROM songplays AS s
LEFT JOIN users AS u
ON s.user_id = u.user_id
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10
```

## Tech Stack
* boto3
* dotenv
* os
* pandas
* psycopg2
* json
