import os
from typing import Optional, List
from pydantic import SecretStr
from pydantic.dataclasses import dataclass
import boto3
from pathlib import Path


@dataclass
class S3:
    region: Optional[str] = None
    access_key_id: Optional[SecretStr] = None
    secret_access_key: Optional[SecretStr] = None

    def __post_init__(self):
        if os.getenv("AWS_ACCESS_KEY_ID") is None or "":
            raise Exception("AWS_ACCESS_KEY_ID env var is not set.")
        if os.getenv("AWS_SECRET_ACCESS_KEY") is None or "":
            raise Exception("AWS_SECRET_ACCESS_KEY env var is not set.")
        self.s3_client = boto3.client("s3")

    def get_s3_client(self):
        return self.s3_client

    def does_bucket_exist(self, bucket_name: str) -> bool:
        """
        :param bucket_name: str -- bucket name you would like to check if exists
        :returns boolean
        """
        list_of_buckets = self.get_list_of_buckets()
        return bucket_name in list_of_buckets

    def download_file(self, bucket_name: str, local_filepath: str | Path, remote_filepath: str | Path) -> None:
        """
        :param bucket_name: str -- s3 bucket you would like to access
        :param local_filepath: str -- save down filepath location
        :param remote_filepath: str -- filepath to the file inside the s3 bucket
        """

        # The filepaths are cast to strings in case they are passed in as a pathlib.Path class.
        self.s3_client.download_file(bucket_name, str(remote_filepath), str(local_filepath))

    def upload_file(self, bucket_name: str, local_filepath: str, remote_filepath: str) -> None:
        """
        :param bucket_name: str -- s3 bucket you would like to access
        :param local_filepath: str -- filepath of the file on your machine you would like to upload
        :param remote_filepath: str -- filepath to location you would like to upload to inside the s3 bucket
        """
        self.s3_client.upload_file(local_filepath, bucket_name, remote_filepath)

    def delete_file(self, bucket_name: str, remote_filepath: str) -> None:
        """
        :param bucket_name: str -- s3 bucket you would like to access
        :param remote_filepath: str -- filepath to the file you would like to delete inside the s3 bucket
        """
        self.s3_client.delete_object(Bucket=bucket_name, Key=remote_filepath)

    def get_file_list(self, bucket_name: str) -> List[str] | List:
        """
        :param bucket_name: str -- s3 bucket you would like to access
        :returns list of files or empty list if bucket is empty
        """
        file_list = []
        try:
            # if Contents key does not exist, key error is thrown and signifies that
            # the bucket is empty.
            s3_files = self.s3_client.list_objects(Bucket=bucket_name)["Contents"]
        except KeyError as e:
            return file_list
        for key in s3_files:
            file_list.append(key["Key"])
        return file_list

    def get_list_of_buckets(self) -> List[str]:
        """
        return list of all buckets in s3\n
        s3 docs -- https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_buckets
        """
        response = self.s3_client.list_buckets()
        bucket_list = []
        for bucket in response["Buckets"]:
            bucket_list.append(bucket["Name"])
        return bucket_list

    def create_bucket(self, bucket_name: str, location: Optional[str] = None) -> None:
        """
        :param bucket_name: str -- bucket name you would like to create
        (bucket_name requires all lowercase and no special characters. Example: 'testing.bucket')
        :parm location: str -- aws region for bucket configuration. Example: 'us-east-1'
        :s3 docs -- https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.create_bucket
        """
        if location:
            self.s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": location})
        else:
            self.s3_client.create_bucket(Bucket=bucket_name)

    def delete_bucket(self, bucket_name: str) -> None:
        """
        :param bucket_name: str -- bucket name you would like to delete
        """
        self.s3_client.delete_bucket(Bucket=bucket_name)

    def generate_download_link_for_file(self, bucket_name: str, file_name: str, time_to_expiry: int = 3600) -> str:
        """
        :param bucket_name: str -- s3 bucket you would like to access
        :param remote_filepath: str -- filepath inside the s3 bucket you would like to create downloadable url
        :param time_to_expiry: int -- time in seconds till link expires, default is 3600s (1 hour)
        """
        url = self.s3_client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": bucket_name, "Key": file_name},
            ExpiresIn=time_to_expiry,
        )
        return url
