import os
from dotenv import load_dotenv
import boto3

load_dotenv()
try:
    sts = boto3.client('sts')
    identity = sts.get_caller_identity()
    print("STS SUCCESS:", identity['Arn'])
except Exception as e:
    print("STS FAILED:", str(e))
