from re import M
import os
from dotenv import load_dotenv
from boto3 import client
from mwaa import MWAA_v2


if __name__ == '__main__':
  load_dotenv()
  ACCESS_KEY = os.environ['AWS_ACCESS_KEY_ID']
  SECRET_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
  SESSION_TOKEN = os.environ['AWS_SESSION_TOKEN']
  mwaa_env_name = os.environ['YOUR_ENVIRONMENT_NAME']

  mwaa_client = client(
    'mwaa',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    aws_session_token=SESSION_TOKEN
  )

  mwaa = MWAA_v2(
    mwaa_client=mwaa_client,
    mwaa_env_name=mwaa_env_name
  )

  print(mwaa.list_dugs())