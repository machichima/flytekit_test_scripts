from flytekit import task, workflow
import datetime

@task
def t1(x: int) -> int:
    return x + 1

@workflow
def wf(start_date: datetime.date | None = None) -> int:
    # This always adds a node
    return t1(x=1)

# @workflow
# def wf(
#     start_date: datetime.date | None = None,
# ) -> None:
#     pass


if __name__ == "__main__":
    import os

    from click.testing import CliRunner

    from flytekit.clis.sdk_in_container import pyflyte

    runner = CliRunner()

    path = os.path.realpath(__file__)
    # curr_workflow = "test_basic_task"
    # curr_workflow = "test_optional_task"
    curr_workflow = "test_complex_task"

    # result = runner.invoke(pyflyte.main, ["run", path, curr_workflow])
    # print("Local Execution: ", result.output)

    result = runner.invoke(pyflyte.main, ["run", "--remote", path, "wf", "--start_date", "2025-06-10"])
    print("Remote Execution: ", result.output)
