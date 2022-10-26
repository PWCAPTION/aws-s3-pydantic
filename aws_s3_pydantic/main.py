import os
from typing import Optional, List, Any, TypeVar
from pydantic import BaseModel
import boto3
from boto3.resources.base import ServiceResource
from pathlib import Path


T = TypeVar("T")


class S3(BaseModel):
    s3_resource: ServiceResource = None
    s3_client: T = None

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        super().__init__(**data)

        if self.s3_client is None:
            if os.getenv("AWS_ACCESS_KEY_ID") is None or "":
                raise Exception("AWS_ACCESS_KEY_ID env var is not set.")
            if os.getenv("AWS_SECRET_ACCESS_KEY") is None or "":
                raise Exception("AWS_SECRET_ACCESS_KEY env var is not set.")
            self.s3_resource = boto3.resource("s3")
            self.s3_client = self.s3_resource.meta.client

    @classmethod
    def default_boto3_resource_init(cls, **kwargs) -> "S3":
        """
        configuring boto3 credentials:\n
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials\n
        boto3 client function:\n
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/boto3.html#boto3.client
        """
        s3_resource = boto3.resource("s3", **kwargs)
        s3_client = s3_resource.meta.client
        return cls(s3_resource=s3_resource, s3_client=s3_client)

    def get_s3_resource(self) -> ServiceResource:
        """
        :return: Boto3 ServiceResource
        """
        return self.s3_resource

    def get_s3_client(self) -> T:
        """
        docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html\n
        :return: Boto3 Client
        """
        return self.s3_client

    def does_bucket_exist(self, bucket_name: str) -> bool:
        """
        :param bucket_name: str -- bucket name you would like to check if exists\n
        :return: boolean
        """
        list_of_buckets = self.get_list_of_buckets()
        return bucket_name in list_of_buckets

    def download_file(self, bucket_name: str, local_filepath: str | Path, remote_filepath: str | Path) -> None:
        """
        :param bucket_name: str -- s3 bucket you would like to access
        :param local_filepath: str -- save down filepath location
        :param remote_filepath: str -- filepath to the file inside the s3 bucket\n
        docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.download_file
        """

        # The filepaths are cast to strings in case they are passed in as a pathlib.Path class.
        self.s3_client.download_file(bucket_name, str(remote_filepath), str(local_filepath))

    def upload_file(self, bucket_name: str, local_filepath: str, remote_filepath: str) -> None:
        """
        :param bucket_name: str -- s3 bucket you would like to access
        :param local_filepath: str -- filepath of the file on your machine you would like to upload
        :param remote_filepath: str -- filepath to location you would like to upload to inside the s3 bucket\n
        docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.upload_file
        """
        self.s3_client.upload_file(local_filepath, bucket_name, remote_filepath)

    def delete_file(self, bucket_name: str, remote_filepath: str) -> None:
        """
        :param bucket_name: str -- s3 bucket you would like to access
        :param remote_filepath: str -- filepath to the file you would like to delete inside the s3 bucket\n
        docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_object
        """
        self.s3_client.delete_object(Bucket=bucket_name, Key=remote_filepath)

    def get_file_list(self, bucket_name: str) -> List[str] | List:
        """
        :param bucket_name: str -- s3 bucket you would like to access
        :return: List[str] or empty list [] if bucket is empty\n
        docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2
        """
        file_list = []
        try:
            # if Contents key does not exist, key error is thrown and signifies that
            # the bucket is empty.
            s3_files = self.s3_client.list_objects_v2(Bucket=bucket_name)["Contents"]
        except KeyError as e:
            return file_list
        for key in s3_files:
            file_list.append(key["Key"])
        return file_list

    def get_file_list_by_dir(self, bucket_name: str, directory_prefix: str) -> List[str] | List:
        # s3 = boto3.resource('s3')
        my_bucket = self.s3_resource.Bucket(bucket_name)

        file_list = []
        for object_summary in my_bucket.objects.filter(Prefix=directory_prefix):
            file_list.append(object_summary.key)
        return file_list

    def get_list_of_buckets(self) -> List[str]:
        """
        Grab list of all buckets in s3

        :return: List[str]\n
        docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_buckets
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
        :param location: str -- aws region for bucket configuration. Example: 'us-east-1'\n
        docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.create_bucket
        """
        if location:
            self.s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": location})
        else:
            self.s3_client.create_bucket(Bucket=bucket_name)

    def delete_bucket(self, bucket_name: str) -> None:
        """
        :param bucket_name: str -- bucket name you would like to delete\n
        docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_bucket
        """
        self.s3_client.delete_bucket(Bucket=bucket_name)

    def generate_download_link_for_file(self, bucket_name: str, file_name: str, time_to_expiry: int = 3600) -> str:
        """
        :param bucket_name: str -- s3 bucket you would like to access
        :param remote_filepath: str -- filepath inside the s3 bucket you would like to create downloadable url
        :param time_to_expiry: int -- time in seconds till link expires, default is 3600s (1 hour)
        :return: str\n
        docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.generate_presigned_url
        """
        url = self.s3_client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": bucket_name, "Key": file_name},
            ExpiresIn=time_to_expiry,
        )
        return url
