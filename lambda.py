import json
import logging
import base64
import boto3
import os
import tarfile
import requests
from io import BytesIO
from datetime import datetime

BUCKET_NAME_STAGE = 'prosodies-landing'
BUCKET_NAME = 'prosodies-bronze'
DATESTR = datetime.now().strftime('%Y%m%d%H%M%S')
AIRFLOW_IP = 'http://34.201.15.124:80'
DAG_ID = 'run_spark_jobs'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

response  = {
    'statusCode': 200,
    'headers': {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Credentials': 'true'
    },
    'body': ''
}

def upload_to_s3(bucket, key, body):
    '''
        Upload file to s3, log errors
    '''
    try:
        s3_response = s3_client.put_object(Bucket=bucket, Key=key, Body=body)   
        logger.info('S3 Response: {}'.format(s3_response))
    except Exception as e:
        raise IOError(e) 

def lambda_handler(event, context):

    # log event, get event header and binary file content
    print("Received event: " + json.dumps(event['headers'], indent=2))
    file_name = event['headers']['file-name']
    file_content = base64.b64decode(event['body'])
    print(file_content)
    
    # if not gzip or txt, do not upload and inform user
    if event['headers']['Content-Type'] not in ['application/x-gzip','text/plain']:
        response['body'] = 'Please upload a gzip or txt file'
        return response
        
    else:
        # partition by datetime
        file_name = f'{DATESTR}/{file_name}'
        
        # upload all files to staging bucket in s3
        upload_to_s3(BUCKET_NAME_STAGE, file_name, file_content)
        
        # if txt, upload as-is to main bucket in s3
        if event['headers']['Content-Type'] == 'text/plain':
            upload_to_s3(BUCKET_NAME, file_name, file_content)
            
        # if tar gz, unzip to main bucket
        else:
            
            # read tar file from staging bucket
            input_tar_file = s3_client.get_object(Bucket = BUCKET_NAME_STAGE, Key = file_name)
            input_tar_content = input_tar_file['Body'].read()
            
            # upload each file within tar file
            with tarfile.open(fileobj = BytesIO(input_tar_content)) as tar:
                for tar_resource in tar:
                    if (tar_resource.isfile()):
                        
                        # extract each file in tar gz and get file name
                        inner_file_bytes = tar.extractfile(tar_resource).read()
                        tar_file = tar_resource.name.split("/")[-1]
                        print(tar_file)
                        
                        # upload file to s3 under datetime partition
                        try:
                            s3_client.upload_fileobj(BytesIO(inner_file_bytes), Bucket = BUCKET_NAME, Key = f'{DATESTR}/{tar_file}')
                        except Exception as e:
                            raise IOError(e) 
        
        # clean up "._" files to prevent glue crawler from reading them    
        bronze_bucket = s3_resource.Bucket(BUCKET_NAME)
        for obj in bronze_bucket.objects.filter(Prefix='20'):
            if obj.key.split("/")[-1].startswith("._"):
                obj.delete()
                
        # set airflow api request parameters
        dag_url = f'{AIRFLOW_IP}/api/experimental/dags/{DAG_ID}/dag_runs'
        headers = {'content-type': 'application/json'}
        conf_val = f'"key":"{DATESTR}"'
        json_data = {'conf': f'{{{conf_val}}}'}
        print(json_data)
        
        # trigger airflow dag using api
        airflow_response = requests.post(dag_url, headers=headers, json=json_data)
        print(airflow_response)
        
        # return aws api-gateway response
        response['body'] = 'Upload successful. Processing file(s).'
        return response