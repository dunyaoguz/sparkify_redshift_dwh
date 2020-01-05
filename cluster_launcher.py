import pandas as pd
import boto3
import json
from dotenv import load_dotenv, find_dotenv
import os

# step 1: create a new IAM user with admin access, add the key and secret access key to the .env file. Rest of the infrastructure work will be done programatically
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

# step 3: create a role that enables redshift to have s3 read access
iam.create_role(RoleName='dwh_project_s3_access', AssumeRolePolicyDocument=json.dumps({'Statement': [{'Action': 'sts:AssumeRole',
                                                                                                      'Effect': 'Allow',
                                                                                                      'Principal': {'Service': 'redshift.amazonaws.com'}}],
                                                                                       'Version': '2012-10-17'}))

# attach s3 read access policy to dwh_project_s3_access
iam.attach_role_policy(RoleName='dwh_project_s3_access', PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
ROLE_ARN = iam.get_role(RoleName='dwh_project_s3_access')['Role']['Arn']

# step 4: create a redshift cluster
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']

redshift.create_cluster(ClusterType='multi-node',
                        NodeType='dc2.large',
                        NumberOfNodes=4,
                        #Identifiers & Credentials
                        DBName='my_dwh',
                        ClusterIdentifier='redshift-cluster-1',
                        MasterUsername=DB_USER,
                        MasterUserPassword=DB_PASSWORD,
                        #Roles (for s3 access)
                        IamRoles=[ROLE_ARN]
)

# wait until cluster status is available, should take 5-10 min. max
print(redshift.describe_clusters(ClusterIdentifier='redshift-cluster-1')['Clusters'][0]['ClusterStatus'])
