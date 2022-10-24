import os
from pytest import fixture
from src.main import S3
from pathlib import Path
from datetime import datetime
import requests

TEST_FILENAME = "test.txt"
S3_BUCKET = "test.aws.s3.utils"
print(f"This S3 Bucket is used for testing: {S3_BUCKET} + <timestamp>")


def get_dt_timestamp() -> str:
    return str(datetime.timestamp(datetime.now()))


def get_unique_bucket_name() -> str:
    return S3_BUCKET + "." + get_dt_timestamp()


@fixture
def s3_client() -> S3:
    return S3()


@fixture(scope="module")
def bucket_name() -> str:
    return get_unique_bucket_name()


def test_create_bucket(s3_client: S3, bucket_name: str) -> None:
    s3_client.create_bucket(bucket_name)
    assert s3_client.does_bucket_exist(bucket_name)


def test_upload_file(s3_client: S3, bucket_name: str) -> None:
    s3_client.upload_file(bucket_name, TEST_FILENAME, TEST_FILENAME)
    assert TEST_FILENAME in s3_client.get_file_list(bucket_name)


def test_download_file(s3_client: S3, bucket_name: str, tmp_path: Path) -> None:
    s3_client.download_file(bucket_name, tmp_path.joinpath(TEST_FILENAME), TEST_FILENAME)
    assert os.path.exists(tmp_path.joinpath(TEST_FILENAME))


def test_generate_download_link_for_file(s3_client: S3, bucket_name: str) -> None:
    url = s3_client.generate_download_link_for_file(bucket_name, TEST_FILENAME)
    response = requests.get(url)
    # b'This is a test file for aws s3 utils.\n' is the conents of the test.txt file
    assert b"This is a test file for aws s3 utils.\n" in response.content


def test_delete_file(s3_client: S3, bucket_name: str) -> None:
    s3_client.delete_file(bucket_name, TEST_FILENAME)
    assert TEST_FILENAME not in s3_client.get_file_list(bucket_name)


def test_get_list_of_buckets(s3_client: S3, bucket_name: str) -> None:
    list_of_buckets = s3_client.get_list_of_buckets()
    assert bucket_name in list_of_buckets


def test_delete_bucket(s3_client: S3, bucket_name: str) -> None:
    s3_client.delete_bucket(bucket_name)
    assert not s3_client.does_bucket_exist(bucket_name)


def test_create_and_delete_bucket_with_location(s3_client: S3, bucket_name: str) -> None:
    bucket_name = get_unique_bucket_name()
    s3_client.create_bucket(bucket_name, location="us-west-1")
    assert s3_client.does_bucket_exist(bucket_name)
    try:
        s3_client.delete_bucket(bucket_name)
    except Exception as e:
        print(f"{e}: {bucket_name}")
    assert not s3_client.does_bucket_exist(bucket_name)
