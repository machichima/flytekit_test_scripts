from flytekit import task, workflow, FlyteRemote
from flytekit.configuration import Config
from flytekit.types.file import FlyteFile
from dataclasses import dataclass
from pathlib import Path


# Custom function to simulate a file validation
def check_file(file_path: Path):
    print(f"Checking file at: {file_path}")
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    if file_path.stat().st_size == 0:
        raise ValueError("File is empty")
    print("File is valid.")


# Define a complex input dataclass
@dataclass
class FlyteBlobComplex:
    blob: FlyteFile
    param: float


# Task that uses the complex input
@task()
def smoke_blob_complex_inputs(val: FlyteBlobComplex) -> bool:
    print("Downloading blob...")
    val.blob.download()
    print(f"Additional param: {val.param}")
    return True


# Workflow to call the task
@workflow
def wf() -> bool:
    blob = FlyteFile("test.txt")
    return smoke_blob_complex_inputs(val=FlyteBlobComplex(blob=blob, param=3.14))


# Run locally (if testing directly via Python)
if __name__ == "__main__":

    ENDPOINT = "localhost:30080"
    PROJECT = "flytesnacks"   # or "default"
    DOMAIN = "development"
    flyte_remote = FlyteRemote(
        config=Config.for_endpoint(endpoint=ENDPOINT, insecure=True),
        default_project=PROJECT,
        default_domain=DOMAIN,
    )
    flyte_remote.execute(
        smoke_blob_complex_inputs,
        inputs={
            "val": {
                "blob": {"path": "./test.txt"},
                "param": 1.0,
            },
        },
        version="latest",
    )

    # from flytekit.clis.sdk_in_container import pyflyte
    # from click.testing import CliRunner
    # import os
    #
    # runner = CliRunner()
    # path = os.path.realpath(__file__)
    #
    # # result = runner.invoke(pyflyte.main, ["run", path, "wf"])
    # # print("Local Execution: ", result.output)
    #
    # result = runner.invoke(pyflyte.main, ["run", "--remote", path, "wf"])
    # print("Remote Execution: ", result.output)
