import boto3

class Boto3BaseClient:
  """
  A base class for AWS clients.

  :param client_name: Name of the boto3 client, such as 'sqs', 'sns', 's3', etc.
  :param profile_name: Name of AWS profile
  """
  def __init__(
      self,
      client_name: str,
      profile_name: str
  ) -> None:
    self._client_name = client_name
    self._profile_name = profile_name
    return

  def __enter__(self):
    session = boto3.Session(profile_name=self._profile_name)
    self.client = session.client(self._client_name)
    return self
  
  def __exit__(self, *_):
    self.client.close()
    return
