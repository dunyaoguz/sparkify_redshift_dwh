import pandas as pd
import boto3
import json
from dotenv import load_dotenv, find_dotenv
import os
from time import sleep
import psycopg2

# step 1: create a new IAM user with admin access from the console, add the key and secret access key to the .env file
# rest of the infrastructure work will be done programatically
load_dotenv()
KEY = os.environ['AWS_KEY']
SECRET = os.environ['AWS_SECRET']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']

# step 2: create s3, iam, redshift clients
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
def create_role():
    ''' Creates an IAM role with s3 read access '''
    iam.create_role(RoleName='dwh_project_s3_access', AssumeRolePolicyDocument=json.dumps({'Statement': [{'Action': 'sts:AssumeRole',
                                                                                                          'Effect': 'Allow',
                                                                                                          'Principal': {'Service': 'redshift.amazonaws.com'}}],
                                                                                           'Version': '2012-10-17'}))
    # attach s3 read access policy to dwh_project_s3_access
    iam.attach_role_policy(RoleName='dwh_project_s3_access', PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")

# step 4: create a redshift cluster
def create_cluster(ROLE_ARN):
    ''' Creates a redshift cluster with 4 dc2.large type nodes'''
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

def check_status(status):
    ''' Checks whether the cluster has the desired status'''
    try:
        while redshift.describe_clusters(ClusterIdentifier='redshift-cluster-1')['Clusters'][0]['ClusterStatus'] != status:
            print('{} cluster'.format(redshift.describe_clusters(ClusterIdentifier='redshift-cluster-1')['Clusters'][0]['ClusterStatus']))
            sleep(15)
        print('cluster is {}'.format(redshift.describe_clusters(ClusterIdentifier='redshift-cluster-1')['Clusters'][0]['ClusterStatus']))
    except:
        print('cluster is deleted')

# step 5: check whether you can connect to the cluster to confirm the set up was successful
def check_connection():
    ''' Checks if a connection can be made to the cluster that was created '''
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(ENDPOINT, 'my_dwh', DB_USER, DB_PASSWORD, PORT))
        cur = conn.cursor()
        cur.execute("CREATE TABLE test (test_id INTEGER)")
        cur.execute("SELECT * FROM test")
        conn.close()
        print('Connection to cluster successful.')
    except Exception as e:
        print('Something went wrong.')

# step 6: delete cluster when you're no longer working with it to avoid additional costs
def reset():
    redshift.delete_cluster(ClusterIdentifier='redshift-cluster-1',
                            SkipFinalClusterSnapshot=True)

def main():
    # create_role()
    ROLE_ARN = iam.get_role(RoleName='dwh_project_s3_access')['Role']['Arn']

    create_cluster(ROLE_ARN)
    # wait until the cluster is created before proceeding further
    check_status('available')

    # make note of the cluster endpoint and port
    ENDPOINT = redshift.describe_clusters(ClusterIdentifier='redshift-cluster-1')['Clusters'][0]['Endpoint']['Address']
    PORT = redshift.describe_clusters(ClusterIdentifier='redshift-cluster-1')['Clusters'][0]['Endpoint']['Port']

    check_connection()

if __name__ == "__main__":
    main()
    # reset()
