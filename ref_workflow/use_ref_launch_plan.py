"""
Use ref plan
"""

import os
from flytekit import task, workflow, reference_task, reference_workflow, reference_launch_plan


@task
def base_list_adder(x: list[int], y: list[int]) -> list[int]:
    return [a + b for a, b in zip(x, y)]

@reference_launch_plan(
    project="flytesnacks",
    domain="development",
    name="use_ref_task.wf",
    version="tqjNfU9CNrIy5B5XhW76cQ",
    # version="{{ registration.version }}",
)
def lp(x: list[int], y: list[int]) -> list[int]:
    return [1, 2, 3]


# @reference_workflow(
#     project="flytesnacks",
#     domain="development",
#     name="ref_workflow.wf",
#     version="FmnTvGOpEnjm2XrxeYF7EA",
# )
@workflow
def wf(x: list[int], y: list[int]) -> list[int]:
    # return lp(x=x, y=y)
    return lp(x, y)
    # return base_list_adder(x=x, y=y)


if __name__ == "__main__":
    from flytekit.clis.sdk_in_container import pyflyte
    from click.testing import CliRunner

    runner = CliRunner()
    path = os.path.realpath(__file__)
    x_val = [1, 2, 3]
    y_val = [4, 5, 6]
    # result = runner.invoke(
    #     pyflyte.main,
    #     ["run", path, "wf", "--x", str(x_val), "--y", str(y_val)],
    # )
    # print("Local Execution: ", result.output)
    result = runner.invoke(
        pyflyte.main,
        ["run", "--remote", path, "wf", "--x", str(x_val), "--y", str(y_val)],
    )
    print("Remote Execution: ", result.output)
