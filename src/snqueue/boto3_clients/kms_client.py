from pydantic import BaseModel
from typing import Optional

from snqueue.boto3_clients import Boto3BaseClient

class KmsDecryptArgs(BaseModel):
  EncryptionContext: Optional[dict[str, str]]
  KeyId: Optional[str]

class KmsClient(Boto3BaseClient):
  """
  Boto3 KMS client.

  :param profile_name: Name of AWS profile
  """
  def __init__(
      self,
      profile_name: str
  ) -> None:
    super().__init__('kms', profile_name)
    return
  
  def decrypt(
      self,
      blob: bytes,
      **kwargs
  ) -> dict:
    """
    Decrypt content that was encrypted by a KMS key.

    :param blob: Data to be decrypted
    :param kwargs: Dictionary of additional args
    :return: Dictionary of decrypted result
    """
    return self.client.decrypt(
      CiphertextBlob=blob,
      **kwargs
    )