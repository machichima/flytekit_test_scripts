import os
from flytekit import task, workflow


@task
def say_hello(name: str) -> str:
    return f"hello {name}!"


@workflow
def sub_wf(name: str = "union"):
    say_hello(name)


@workflow
def wf(name: str = "union"):
    sub_wf(name)


if __name__ == "__main__":
    from flytekit.clis.sdk_in_container import pyflyte
    from click.testing import CliRunner

    runner = CliRunner()
    path = os.path.realpath(__file__)
    name = "flyte"
    result = runner.invoke(
        pyflyte.main,
        ["run", path, "wf", "--name", name],
    )
    print("Local Execution: ", result.output)
    # result = runner.invoke(
    #     pyflyte.main,
    #     ["run", "--remote", path, "wf", "--name", name],
    # )
    # print("Remote Execution: ", result.output)
