import streamlit as st
import pandas as pd
import boto3
import os
from io import BytesIO
import requests

# Configure AWS S3
region_name = 'ap-south-1'
bucket_name = 'excel-integrator-inventory-bucket'

s3_client = boto3.client(
    's3',
    region_name=region_name,
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

def generate_presigned_url(bucket_name, object_name, expiration=3600):
    response = s3_client.generate_presigned_url('put_object',
                                                Params={'Bucket': bucket_name, 'Key': object_name},
                                                ExpiresIn=expiration)
    return response

def upload_to_s3_via_presigned_url(url, data):
    headers = {'Content-Type': 'application/octet-stream'}
    response = requests.put(url, data=data, headers=headers)
    response.raise_for_status()

def main():
    st.title("Excel Integrator")
    st.write("Upload an Excel file to process")

    uploaded_file = st.file_uploader("Choose an Excel file", type=["xlsx"])

    if uploaded_file is not None:
        file_name = uploaded_file.name.replace(" ", "_")
        input_file_name = f"input/{file_name}"
        print(input_file_name)
        
        # Generate presigned URL
        upload_url = generate_presigned_url(bucket_name, input_file_name)
        print(upload_url)
        
        # Upload file using presigned URL
        upload_to_s3_via_presigned_url(upload_url, uploaded_file.getvalue())
        st.write(f"Uploaded {file_name} to S3 bucket {bucket_name}/input")


if __name__ == "__main__":
    main()