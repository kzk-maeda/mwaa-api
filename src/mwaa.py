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

  def list_dugs(self) -> str:
    '''
    https://airflow.apache.org/docs/apache-airflow/2.0.2/cli-and-env-variables-ref.html#list_repeat2
    '''
    raw_data = "dags list -o json"
    out = self._run(raw_data)

    return out
  
  def list_runs(self) -> str:
    '''
    https://airflow.apache.org/docs/apache-airflow/2.0.2/cli-and-env-variables-ref.html#list-runs
    '''
    raw_data = 'dags list-runs -o json'
    out = self._run(raw_data)

    return out
  
  def trigger(self, dag_id: str) -> str:
    '''
    https://airflow.apache.org/docs/apache-airflow/2.0.2/cli-and-env-variables-ref.html#trigger
    '''
    raw_data = f'dags trigger {dag_id}'
    out = self._run(raw_data)

    return out
  
  def state(self, dag_id: str, run_id: str) -> str:
    '''
    https://airflow.apache.org/docs/apache-airflow/2.0.2/cli-and-env-variables-ref.html#state
    '''
    raw_data = f'dags state'
    out = self._run(raw_data)

    return out
  
  def _run(self, raw_data) -> str:
    '''
    Common method called to run API to MWAA
    '''
    response = requests.post(
      self.mwaa_webserver_hostname,
      headers={
        'Authorization': self.mwaa_auth_token,
        'Content-Type': 'text/plain'
      },
      data=raw_data
    )
    response.raise_for_status()

    mwaa_std_err_message = base64.b64decode(response.json()['stderr']).decode('utf8')
    mwaa_std_out_message = base64.b64decode(response.json()['stdout']).decode('utf8')

    if mwaa_std_err_message:
      print(mwaa_std_err_message)
      raise
    
    return mwaa_std_out_message