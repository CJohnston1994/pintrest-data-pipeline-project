import os, logging
from dotenv import load_dotenv

def AWS_CREDENTIALS():
    load_dotenv()
    return {'AWS_ACCESS_KEY':os.environ.get('S3_ACCESS_KEY'), 'AWS_SECRET_KEY':os.getenv('S3_SECRET_KEY')}

BUCKET_NAME = 'pinterest-data-0759ba42-ccf0-4396-9b86-76de3b8e6640'
AWS_REIGON = 'eu-west-1'