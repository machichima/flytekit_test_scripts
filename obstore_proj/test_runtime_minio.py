"""
Test get and put data from minio
- runtime
- memory
"""

import os
import time

from pathlib import Path
from typing import List

from flytekit import task, workflow, current_context, ImageSpec
from flytekit.types.file import FlyteFile


# Remote path
remote_path_from = "s3://my-s3-bucket/test.json"
remote_path_to = "s3://my-s3-bucket/test_new.json"

remote_base_url = "s3://my-s3-bucket/"

def measure_runtime(func, *args, **kwargs):
    """
    Measure the runtime of a given function with its arguments.

    Args:
        func (callable): The function to measure.
        *args: Positional arguments for the function.
        **kwargs: Keyword arguments for the function.

    Returns:
        tuple: The result of the function and the runtime in seconds.
    """
    start_time = time.time()  # Start the timer
    result = func(*args, **kwargs)  # Call the function
    end_time = time.time()  # End the timer

    runtime = end_time - start_time
    return result, runtime


@task
def put_file(input_file_path: str, output_location: str = "") -> FlyteFile:

    print(current_context().working_directory)  # /tmp/...
    remote_out_path = os.path.join(
        output_location,
        input_file_path.split("/")[1],
    )
    print(f"remote out path: {remote_out_path}")
    
    # can't I do this?
    return FlyteFile(path=input_file_path, remote_path=remote_out_path)


@workflow
def wf():

    file_li = [
        "mock_files/mock_file_100mb.txt",
        "mock_files/mock_file_200mb.txt",
        "mock_files/mock_file_300mb.txt",
        "mock_files/mock_file_400mb.txt",
        "mock_files/mock_file_500mb.txt",
        "mock_files/mock_file_600mb.txt",
        "mock_files/mock_file_700mb.txt",
        "mock_files/mock_file_800mb.txt",
        "mock_files/mock_file_900mb.txt",
        "mock_files/mock_file_1000mb.txt",
        "mock_files/mock_file_1100mb.txt",
        "mock_files/mock_file_1200mb.txt",
        "mock_files/mock_file_1300mb.txt",
        "mock_files/mock_file_1400mb.txt",
        "mock_files/mock_file_1500mb.txt",
        "mock_files/mock_file_1600mb.txt",
        "mock_files/mock_file_1700mb.txt",
        "mock_files/mock_file_1800mb.txt",
        "mock_files/mock_file_1900mb.txt",
        "mock_files/mock_file_2000mb.txt",
    ]

    import pandas as pd
    runtime_li = []

    for file in file_li:
        # flyte_file = FlyteFile(path=str(file)) 
        print(f"file: {file}")

        start_time = time.time()  # Start the timer
        put_file(file, remote_base_url)
        end_time = time.time()  # End the timer

        runtime = end_time - start_time
        print(f"runtime: {runtime}")
        runtime_li.append(runtime)

    df = pd.DataFrame({"obstore": runtime_li})
    df.to_csv("./runtime_measure_origin.csv")

    # return out_file


if __name__ == "__main__":
    from flytekit.clis.sdk_in_container import pyflyte
    from click.testing import CliRunner

    mock_folder_path = Path("./mock_files")


    runner = CliRunner()
    path = os.path.realpath(__file__)
    result = runner.invoke(pyflyte.main, ["run", path, "wf"])
    print("Local Execution: ", result.output)
    # result = runner.invoke(pyflyte.main, ["run", "--remote", path, "wf"])
    # print("Remote Execution: ", result.output)
