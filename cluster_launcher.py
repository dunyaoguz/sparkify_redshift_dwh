import pandas as pd
import boto3
import json
from dotenv import load_dotenv, find_dotenv
import os

# step 1: create a new IAM user with admin access, add the key and secret access key to the config file
# step 2: create s3, iam, redshift clients

load_dotenv()
KEY = os.environ['AWS_KEY']
SECRET = os.environ['AWS_SECRET']

s3 = boto3.client('s3',
                  region_name="us-west-2",
                  aws_access_key_id=KEY,
                  aws_secret_access_key=SECRET
                   )

iam = boto3.client('iam',
                   region_name='us-west-2',
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET,
                   )

redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )
