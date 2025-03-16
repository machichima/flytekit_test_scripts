"""
Use ref task
"""

import os
from flytekit import task, workflow, reference_task, reference_workflow


@reference_task(
    project="flytesnacks",
    domain="development",
    name="ref_workflow.base_list_adder",
    version="zYGvqxn4gUnAC3tiCLM2tQ",
)
def base_list_adder(x: list[int], y: list[int]) -> list[int]:
    ...


# @reference_workflow(
#     project="flytesnacks",
#     domain="development",
#     name="ref_workflow.wf",
#     version="FmnTvGOpEnjm2XrxeYF7EA",
# )
@workflow
def wf(x: list[int], y: list[int]) -> list[int]:
    return base_list_adder(x, y)


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
