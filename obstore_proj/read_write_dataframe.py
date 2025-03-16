import os

from pathlib import Path
import logging
import flytekit
from flytekit import (
    task,
    workflow,
    FlyteContextManager,
    Resources,
    WorkflowFailurePolicy,
    StructuredDataset,
    ImageSpec,
)
from flytekit.types.file import FlyteFile

# import hunter
# from hunter import trace, Q, CodePrinter
#
# # Assuming your working directory is the current directory:
# working_dir = Path(os.getcwd()).parent.parent
# print(working_dir)
#
# # Set up hunter to trace only files in your working directory
# def flat_printer(event):
#     print(f"{event.filename}:{event.lineno} => {event.function}()")
# trace(
#     Q(filename__contains=str("flytekit_obstore")),  # Tracks only modules whose path starts with your working directory
#     action=flat_printer,
#     kind="call"
# )


resource = Resources(cpu="2", mem="4096Mi")
directory = "./test"
# for aws s3
# DEFAULT_REMOTE_PATH = "s3://flyte-aws-s3-bucket-tokyo/test/file.txt"
# DEFAULT_REMOTE_STRUCTURE_DATA_PATH = "s3://flyte-aws-s3-bucket-tokyo/test/file.csv"
# for minio
DEFAULT_REMOTE_PATH = "s3://my-s3-bucket/test/file.txt"
DEFAULT_REMOTE_STRUCTURE_DATA_PATH = "s3://my-s3-bucket/test/file.csv"
RUSTFS_IMAGE = "ghcr.io/unionai-oss/flytekit-rustfs:latest"

new_flytekit = "git+https://github.com/machichima/flytekit.git@31015e050869ae8db2a485f201dd46a07746c3d3"
obstore = "git+https://github.com/machichima/obstore.git@change-py-10#subdirectory=obstore"
obstore_image_spec = ImageSpec(
    registry="localhost:30000",
    base_image="rust:1.80-slim",
    packages=[
        new_flytekit,
        "pyarrow",
        "pandas",
        obstore,
    ],
    apt_packages=["git", "cargo"],
    # commands=[
    #     "curl https://sh.rustup.rs -sSf | sh -s -- -y",
    #     'source "$HOME/.cargo/env"',
    #     "rustc -V",
    # ],
)


master_flytekit = "git+https://github.com/flyteorg/flytekit.git@4e93e36843b8f13f06eb088e1a46232ad1fb225d"
flyte_image_spec = ImageSpec(
    registry="pingsutw",
    packages=[master_flytekit, "pyarrow", "pandas"],
    apt_packages=["git"],
)


def upload_file(size_mb: int) -> FlyteFile:
    os.makedirs(directory, exist_ok=True)
    file_path = os.path.join(directory, "file.txt")

    size_in_bytes = size_mb * 1024 * 1024  # Convert MB to bytes
    with open(file_path, "wb") as f:
        f.write(b"0" * size_in_bytes)

    if flytekit.current_context().execution_id.domain == "local":
        remote_path = DEFAULT_REMOTE_PATH
    else:
        ctx = FlyteContextManager.current_context()
        remote_path = ctx.file_access.get_random_remote_path()
    print(f"uploading {file_path} to {remote_path}.")
    return FlyteFile(file_path, remote_path=remote_path)


def download_file(f: FlyteFile):
    f._downloader()


@task(container_image=flyte_image_spec, requests=resource)
def upload_file_with_fsspec(size_mb: int) -> FlyteFile:
    return upload_file(size_mb=size_mb)


@task(container_image=flyte_image_spec, requests=resource)
def download_file_with_fsspec(f: FlyteFile):
    download_file(f=f)


@task(container_image=obstore_image_spec, requests=resource)
def upload_file_with_obstore(size_mb: int) -> FlyteFile:
    return upload_file(size_mb=size_mb)


@task(container_image=obstore_image_spec, requests=resource)
def download_file_with_obstore(f: FlyteFile):
    download_file(f=f)


@task(container_image=flyte_image_spec, requests=resource)
def write_dataframe_with_fsspec(row: int) -> StructuredDataset:
    import pandas as pd
    import numpy as np

    data = {}
    for i in range(10):
        col_name = f"col{i+1}"
        data[col_name] = np.random.randint(low=0, high=100, size=row)
    df = pd.DataFrame(data)
    return StructuredDataset(dataframe=df, uri = DEFAULT_REMOTE_STRUCTURE_DATA_PATH)


@task(container_image=flyte_image_spec, requests=resource)
def read_dataframe_with_fsspec(df: StructuredDataset):
    import pandas as pd

    print(df.open(pd.DataFrame).all())


@task(container_image=obstore_image_spec, requests=resource)
def write_dataframe_with_obstore(row: int) -> StructuredDataset:
    import pandas as pd
    import numpy as np

    data = {}
    for i in range(10):
        col_name = f"col{i+1}"
        data[col_name] = np.random.randint(low=0, high=100, size=row)
    df = pd.DataFrame(data)
    print("uploaded dataframe")
    return StructuredDataset(dataframe=df, uri=DEFAULT_REMOTE_STRUCTURE_DATA_PATH)


@task(container_image=obstore_image_spec, requests=resource)
def read_dataframe_with_obstore(df: StructuredDataset):
    import pandas as pd

    print(df.open(pd.DataFrame).all())


@workflow(failure_policy=WorkflowFailurePolicy.FAIL_AFTER_EXECUTABLE_NODES_COMPLETE)
def wf(size_mb: int = 50):
    # f1 = upload_file_with_fsspec(size_mb=size_mb)
    # download_file_with_fsspec(f=f1)
    #
    # f2 = upload_file_with_obstore(size_mb=size_mb)
    # # f2 = FlyteFile(path=DEFAULT_REMOTE_PATH)
    # download_file_with_obstore(f=f2)

    df1 = write_dataframe_with_fsspec(row=5)
    read_dataframe_with_fsspec(df=df1)

    # df2 = write_dataframe_with_obstore(row=5)
    # read_dataframe_with_obstore(df=df2)


if __name__ == "__main__":
    from flytekit.clis.sdk_in_container import pyflyte
    from click.testing import CliRunner

    runner = CliRunner()
    path = os.path.realpath(__file__)
    result = runner.invoke(pyflyte.main, ["run", path, "wf"])
    print("Local Execution: ", result.output)
    # result = runner.invoke(pyflyte.main, ["run", "--remote", path, "wf"])
    # print("Remote Execution: ", result.output)
