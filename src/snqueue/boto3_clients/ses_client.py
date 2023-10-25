from snqueue.boto3_clients import Boto3BaseClient

class SesClient(Boto3BaseClient):
  """
  SES client.

  :param profile_name: Name of AWS profile
  """
  def __init__(
      self,
      profile_name: str
  ) -> None:
    super().__init__('ses', profile_name)
    return
  
  def send_raw_email(
      self,
      raw_message: bytes
  ) -> dict:
    """
    Send a raw message.

    :param raw_message: bytes
    :return: Dictionary of the response
    """
    return self.client.send_raw_email(RawMessage={'Data': raw_message})