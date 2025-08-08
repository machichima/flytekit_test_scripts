# pyflyte run --env AWS_ACCESS_KEY_ID= --env AWS_SECRET_ACCESS_KEY= --remote check_s3.py build_wf

import os
import polars as pl
import flytekit as fl

polars_image = fl.ImageSpec(
    registry="localhost:30000",
    name="polars_docker_flyte-1",
    platform="linux/amd64",
    packages=[
        "pandas",
        "pyarrow",
        "polars",
        "flytekitplugins-polars"
    ],
)


@fl.workflow()
def build_wf():
    s3_file = "s3://flyte-aws-s3-bucket-tokyo/test/titanic.parquet"
    get_stats(fl.StructuredDataset(uri=s3_file, format="parquet"))


@fl.task(container_image=polars_image)
def get_stats(sd: fl.StructuredDataset):
    # Get the local path to the Parquet file
    df = sd.open(pl.DataFrame).all()
    # df = sd.open(pd.DataFrame).all()
    # if sd.uri is None:
    #     raise ValueError("Expected local path in StructuredDataset.uri")
    #
    # print("Local path from StructuredDataset:", sd.uri)
    #
    # # Use Polars scan_parquet on local file
    # df = pl.scan_parquet(sd.uri).collect()
    print(df.describe())


if __name__ == "__main__":
    get_stats()
