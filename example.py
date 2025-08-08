import os
from flytekit import task

@task()
def example_task(flag: bool) -> bool:
    return flag


if __name__ == "__main__":
    from flytekit.clis.sdk_in_container import pyflyte
    from click.testing import CliRunner

    runner = CliRunner()
    path = os.path.realpath(__file__)
    result = runner.invoke(pyflyte.main, ["run", "remote-task", "example.example_task", "--no-flag"])
    print("Local Execution: ", result.output)
    # result = runner.invoke(pyflyte.main, ["run", "--remote", path, "wf"])
    # print("Remote Execution: ", result.output)
