import os, logging
from dotenv import load_dotenv
load_dotenv()

def AWS_CREDENTIALS():
    return {'AWS_ACCESS_KEY':os.environ.get('S3_ACCESS_KEY'), 'AWS_SECRET_KEY':os.getenv('S3_SECRET_KEY')}

BUCKET_NAME = os.environ.get('BUCKET_NAME')
AWS_REGION = os.environ.get('BUCKET_REGION')