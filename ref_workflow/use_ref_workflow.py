"""
invoke the ref workflow

This will fail by running the command:
pyflyte -vv -c ~/.flyte/config-sandbox.yaml run --remote using_references.py base_lists_wf --x "[1,2,3]" --y "[1,2,3]"
"""

import os
from flytekit import reference_workflow


@reference_workflow(
    project="flytesnacks",
    domain="development",
    name="ref_workflow.wf",
    version="FmnTvGOpEnjm2XrxeYF7EA",
)
def wf(x: list[int], y: list[int]):
    # return [2, 2, 2]
    ...

# def wf(x: list[int], y: list[int]): ...


if __name__ == "__main__":
    from flytekit.clis.sdk_in_container import pyflyte
    from click.testing import CliRunner

    runner = CliRunner()
    path = os.path.realpath(__file__)
    # x_val = [1, 2, 3]
    x_val = [4, 5, 6]
    y_val = [4, 5, 6]
    result = runner.invoke(pyflyte.main, ["run", path, "wf", "--x", str(x_val), "--y", str(y_val) ])
    print("Local Execution: ", result.output)

    result = runner.invoke(pyflyte.main, ["run", "--remote", path, "wf", "--x", str(x_val), "--y", str(y_val) ])
    print("Remote Execution: ", result.output)
