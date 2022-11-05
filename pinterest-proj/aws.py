import boto3, logging
import config as c

logging.basicConfig(level=logging.INFO

class Boto3_Connection():
    def __init__(self,):
        s3_session = boto3.Session(aws_access_key_id=c.AWS_ACCESSS_KEY,aws_secret_access_key=c.AWS_SECRET_KET)
        self.s3_client = s3_session.client('s3')
        with open('s3_name.txt', 'r') as f:
            self.bucket_name = f.read()
            logging.info(f"bucket name: {self.bucket_name}")
            
    def send(self, data:json):
        self.s3_client.put_object(data)