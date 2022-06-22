import base64
import requests

from dotenv import load_dotenv
from boto3 import client

class MWAA_v2:
  def __init__(self, mwaa_client: client, mwaa_env_name: str) -> None:
    self.mwaa_client = mwaa_client

    mwaa_cli_token = self.mwaa_client.create_cli_token(
      Name = mwaa_env_name
    )
    self.mwaa_auth_token = 'Bearer ' + mwaa_cli_token['CliToken']
    self.mwaa_webserver_hostname = 'https://{0}/aws_mwaa/cli'.format(mwaa_cli_token['WebServerHostname'])

  def list_dugs(self):
    raw_data = "dags list -o json"
    err, out = self._run(raw_data)
    if err:
      print(err)
      raise

    return out
  
  def list_runs(self):
    raw_data = 'dags list-runs -o json'
    err, out = self._run(raw_data)
    if err:
      print(err)
      raise

    return out
  
  def _run(self, raw_data):
    response = requests.post(
      self.mwaa_webserver_hostname,
      headers={
        'Authorization': self.mwaa_auth_token,
        'Content-Type': 'text/plain'
      },
      data=raw_data
    )

    mwaa_std_err_message = base64.b64decode(response.json()['stderr']).decode('utf8')
    mwaa_std_out_message = base64.b64decode(response.json()['stdout']).decode('utf8')
    
    return mwaa_std_err_message, mwaa_std_out_message