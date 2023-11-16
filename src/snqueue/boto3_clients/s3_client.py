from typing import IO

from snqueue.boto3_clients import Boto3BaseClient

class S3Client(Boto3BaseClient):
  """
  AWS S3 client.

  :param profile_name: name of AWS profile
  """
  def __init__(
      self,
      profile_name: str
  ) -> None:
    super().__init__('s3', profile_name)
    return
  
  def download(
      self,
      bucket_name: str,
      object_key: str,
      file: IO
  ) -> None:
    """
    Download an S3 object

    :param bucket_name: string
    :param object_key: string
    :param file: File object to store the downloaded S3 object
    """
    self.client.download_fileobj(bucket_name, object_key, file)
    return
  
  def get_metadata(
      self,
      bucket_name: str,
      object_key: str
  ) -> dict:
    """
    Get metadata of an S3 object

    :param bucket_name: string
    :param object_key: string
    :return: Dictionary of the metadata
    """
    head = self.client.head_object(Bucket=bucket_name, Key=object_key)
    return head['Metadata']
  
  def create_presigned_get(
      self,
      bucket_name: str,
      object_key: str,
      expiration: int=3600
  ) -> str | None:
    """
    Generate a presigned URL to share an S3 object

    :param bucket_name: string
    :param object_key: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string
    :raises:
      botocore.exceptions.ClientError: If anything wrong
    """
    # Generate a presigend URL for the S3 object
    return self.client.generate_presigned_url(
      'get_object',
      Params={'Bucket': bucket_name, 'Key': object_key},
      ExpiresIn=expiration
    )
  
  def create_presigned_post(
      self,
      bucket_name: str,
      object_key: str,
      fields: dict=None,
      conditions: list=None,
      expiration: int=3600
  ) -> dict | None:
    """
    Generate a presigned URL S3 POST request to upload a file

    :param bucket_name: string
    :param object_key: string
    :param fields: Dictionary of prefilled form fields
    :param conditions: List of conditions to include in the policy
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Dictionary with the following keys:
      url: URL to post to
      fields: Dictionary of form fields and values to submit with the POST
    :raises:
      botocore.exceptions.ClientError: If anything wrong
    """
    # Generate a presigned S3 POST URL
    return self.client.generate_presigned_post(
      bucket_name,
      object_key,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=expiration
    )