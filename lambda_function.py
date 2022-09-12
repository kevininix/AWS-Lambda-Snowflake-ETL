import pandas as pd
# For AWS
import boto3
import io
from io import StringIO
import os

key_id = os.environ.get('key_id')
secret_key = os.environ.get('secret_key')

def lambda_handler(event, context):
    
    # Get name of the csv file
    s3_file_key = event['Records'][0]['s3']['object']['key']
    bucket = 'kevininisourcefiles'

    s3 = boto3.client('s3', aws_access_key_id = key_id, 
    aws_secret_access_key = secret_key)
    # Creating an object from the csv file
    obj = s3.get_object(Bucket = bucket, Key = s3_file_key)
    # Turning it into a dataframe
    initial_df = pd.read_csv(io.BytesIO(obj['Body'].read()))

    s3_resource = boto3.resource(

        service_name = 's3',
        region_name = 'ca-central-1',
        aws_access_key_id = key_id,
        aws_secret_access_key = secret_key
    )

    # Filter df to only show setosa flowers and load it
    # into the external stage bucket
    bucket ='kevininidestination/data/';
    df = initial_df[(initial_df.species == "setosa")];
    csv_buffer = StringIO()
    df.to_csv(csv_buffer,index=False);
    s3_resource.Object(bucket, s3_file_key).put(Body = csv_buffer.getvalue())

