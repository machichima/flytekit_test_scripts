# pyflyte run --env AWS_ACCESS_KEY_ID= --env AWS_SECRET_ACCESS_KEY= --remote check_s3.py build_wf

import os
import requests
from time import sleep, time
import polars as pl
import flytekit as fl
import s3fs
import pyarrow.fs
from pydantic_settings import BaseSettings, SettingsConfigDict

import pandas as pd


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # AWS_ENDPOINT: str = ""
    AWS_ACCESS_KEY_ID: str = ""
    AWS_SECRET_ACCESS_KEY: str = ""
    AWS_REGION: str = "ap-northeast-1"


settings = Settings(_env_file=".env")

polars_image = fl.ImageSpec(
    registry="localhost:30000",
    name="polars_docker_flyte-base-1",
    platform="linux/amd64",
    # base_image="python:3.12-slim",
    packages=[
        "pydantic_settings",
        "pandas",
        "pyarrow",
        "boto3",
        "polars",
        "s3fs",
    ],
    apt_packages=[
        "ca-certificates"
    ],
)


@fl.workflow()
def build_wf():
    get_stats()


@fl.task(container_image=polars_image)
def get_stats():
    #
    # import ssl
    # print(ssl.get_default_verify_paths())

    # sleep(12000000)

    # boto
    import boto3, botocore
    s3 = boto3.client("s3", region_name="ap-northeast-1")
    try:
        s3.head_object(Bucket="flyte-aws-s3-bucket-tokyo", Key="test/titanic.parquet")
        print("boto3 HEAD succeeded")
    except botocore.exceptions.ClientError as e:
        print("boto3 HEAD failed:", e)


    url = "https://flyte-aws-s3-bucket-tokyo.s3.ap-northeast-1.amazonaws.com/test/titanic.parquet"
    r = requests.head(url)
    print(r.status_code)

    aws_key = os.environ["AWS_ACCESS_KEY_ID"]
    aws_secret = os.environ["AWS_SECRET_ACCESS_KEY"]
    # aws_key = settings.AWS_ACCESS_KEY_ID
    # aws_secret = settings.AWS_SECRET_ACCESS_KEY

    for var in ["FLYTE_AWS_ENDPOINT", "FLYTE_AWS_ACCESS_KEY_ID", "FLYTE_AWS_SECRET_ACCESS_KEY"]:
        os.environ.pop(var, None)

    print("All environment variables:")
    for k, v in os.environ.items():
        print(f"{k}={v}")

    s3_file = "s3://flyte-aws-s3-bucket-tokyo/test/titanic.parquet"
    # s3_file = "s3://flyte-aws-s3-bucket-tokyo/test/*.parquet"

    # Try initializing Arrow's S3FileSystem explicitly
    print("pyarrow")
    try:
        fs = pyarrow.fs.S3FileSystem(region="ap-northeast-1")
        info = fs.get_file_info("flyte-aws-s3-bucket-tokyo/test/titanic.parquet")
        print("Arrow S3 File Info:", info)
    except Exception as e:
        print("Arrow S3 HEAD FAILED:", str(e))

    # Polars
    fs = pyarrow.fs.S3FileSystem(
        access_key=os.environ["AWS_ACCESS_KEY_ID"],
        secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region="ap-northeast-1",
    )

    # This tells Polars: "use this filesystem, don't create your own"
    # df = pl.read_parquet(s3_file, pyarrow_options={"filesystem": fs})
    with fs.open_input_file("flyte-aws-s3-bucket-tokyo/test/titanic.parquet") as f:
        # Read the file from the in-memory file object
        df = pl.scan_parquet(f)
    # print(df.describe())

    storage_option = {
        "key": aws_key,
        "secret": aws_secret,
        "client_kwargs": {"region_name": "ap-northeast-1"},
    }

    fs = s3fs.S3FileSystem(**storage_option)

    print(fs.ls("s3://flyte-aws-s3-bucket-tokyo/test/"))

    print("Using pandas")
    print("pandas storage option: ", storage_option)
    # Pandas
    df = pd.read_parquet(
        s3_file,
        storage_options=storage_option,
    )
    print(df.describe())

    print("Using polars")
    # s3_file = "s3://nyc-tlc/green_tripdata_2020-01.parquet"
    df = pl.scan_parquet(
        s3_file,
        storage_options={
            "aws_access_key_id": aws_key,
            "aws_secret_access_key": aws_secret,
            "aws_region": "ap-northeast-1",
            # "aws_endpoint": "https://s3.ap-northeast-1.amazonaws.com",
            # "aws_bucket": "flyte-aws-s3-bucket-tokyo",
            # "virtual_hosted_style_request": "true",
        },
    )  # .collect()
    print(df.describe())


if __name__ == "__main__":
    get_stats()
