from flytekit import task, workflow

# Defining an optional dict with list[str] values works


@task
def basic_task(data: dict[str, list[str]] | None) -> None:
    return


@workflow
def test_basic_task() -> None:
    return basic_task(data={"key": ["value1", "value2"]})


# Defining an dict with optional list[str] values works


@task
def optional_task(data: dict[str, list[str] | None]) -> None:
    return


@workflow
def test_optional_task() -> None:
    return optional_task(data={"key": ["value1", "value2"]})


# Defining an optional dict with optional list[str] values
# DOES NOT work locally, but it does work on a cluster


@task
def complex_task(data: dict[str, list[str] | None] | None) -> dict[str, list[str] | None] | None:
    return data


# @workflow
def test_complex_task() -> dict[str, list[str] | None] | None:
    return complex_task(data={"key": ["value1", "value2"]})


if __name__ == "__main__":
    print(test_complex_task())
    # import os
    #
    # from click.testing import CliRunner
    #
    # from flytekit.clis.sdk_in_container import pyflyte
    #
    # runner = CliRunner()
    #
    # path = os.path.realpath(__file__)
    # # curr_workflow = "test_basic_task"
    # # curr_workflow = "test_optional_task"
    # curr_workflow = "test_complex_task"
    #
    # result = runner.invoke(pyflyte.main, ["run", path, curr_workflow])
    # print("Local Execution: ", result.output)
    #
    # result = runner.invoke(pyflyte.main, ["run", "--remote", path, curr_workflow])
    # print("Remote Execution: ", result.output)
