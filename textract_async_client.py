import boto3
import csv
import time
import json


def read_from_csv():
    file_name = 'aws_access.csv'
    with open(file_name, 'r') as csvfile:
    # creating a csv reader object
        csvreader = csv.reader(csvfile)
        fields = next(csvreader)
        for row in csvreader:
            return row[0],row[1]


aws_access_key_id, aws_secret_access_key = read_from_csv()
file_name = 'form_60.png'
file_path='/Users/arjun/Documents/Projects/aws_poc/files/form_60.png'
bucket = 'textract-client'


s3 = boto3.client(
    's3', 
    region_name='us-east-1', 
    aws_access_key_id=aws_access_key_id, 
    aws_secret_access_key=aws_secret_access_key
)

s3.upload_file(file_path, bucket, file_name)
print('file uploaded to s3!')

textract = boto3.client(
    'textract', 
    region_name='us-east-1', 
    aws_access_key_id=aws_access_key_id, 
    aws_secret_access_key=aws_secret_access_key
)


def upload_doc():

    doc_spec = {"S3Object": {"Bucket": bucket, "Name": file_name}}

    response = textract.start_document_analysis(
        DocumentLocation=doc_spec, FeatureTypes=["FORMS","TABLES"]
    )
    return response["JobId"]

def download_doc(job_id, initial_delay, poll_interval, max_attempts):
    time.sleep(initial_delay)
    attempt = 0
    job_status = None
    response = None
    while attempt < max_attempts:
        response = textract.get_document_analysis(JobId=job_id)
        job_status = response["JobStatus"]

        if job_status != "IN_PROGRESS":
            break

        time.sleep(poll_interval)  # Remember that `get` attempts are throttled.
        attempt += 1
    with open(f'{job_id}.json', 'w') as f:
        json.dump(response, f)
    return job_status


job_id = upload_doc()
print(job_id)
download_doc(job_id,10,2.5,50)
print('file downloaded')
s3.delete_object(
    Bucket=bucket,
    Key=file_name
)
print('file deleted from s3!')