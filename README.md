# aws-s3-pydantic
A boto3 s3 client with better typing.

# install
```
poetry add git+https://github.com/PWCAPTION/aws-s3-pydantic.git#0.0.5
```

# How to use
```python
from aws_s3_pydantic import S3

# credentials are set as env vars:
# AWS_ACCESS_KEY_ID
# AWS_SECRET_ACCESS_KEY
s3 = S3()
s3.get_list_of_buckets()
```
