import os

from flytekit import task, workflow
from flytekit.types.file import FlyteFile


# Remote path
remote_path = "s3://my-s3-bucket/test/test.txt"
# remote_path = "./test.json"


@task
def create_ff(input_file: FlyteFile) -> FlyteFile:
    with open(input_file, "r") as f:
        content = f.read()
        print(content)

    return input_file

@workflow
def wf() -> FlyteFile:
    existed_file = FlyteFile(path=remote_path)
    result_file = create_ff(input_file=existed_file)
    # result_file = task_remove_column(input_file=shuffled_file, column_name="County")
    return result_file


if __name__ == "__main__":
    from flytekit.clis.sdk_in_container import pyflyte
    from click.testing import CliRunner

    runner = CliRunner()
    path = os.path.realpath(__file__)
    result = runner.invoke(pyflyte.main, ["run", path, "wf"])
    print("Local Execution: ", result.output)
    # result = runner.invoke(pyflyte.main, ["run", "--remote", path, "wf"])
    # print("Remote Execution: ", result.output)
