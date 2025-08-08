import typing

import flytekit as fl
from flytekit import WorkflowFailurePolicy
from flytekit.types.error.error import FlyteError


@fl.task
def create_cluster(name: str):
    print(f"Creating cluster: {name}")


# Create a task that will fail during execution
@fl.task
def t1(a: int, b: str):
    print(f"{a} {b}")
    raise ValueError("Fail!")


@fl.task
def clean_up(name: str, err: typing.Optional[FlyteError] = None):
    print(f"Deleting cluster {name} due to {err}")


# @fl.workflow
# def wf(a: int, b: str):
#     create_cluster(name=f"cluster-{a}")
#     t1(a=a, b=b)


# In this case, both parent and child workflows will fail,
# resulting in the `clean_up` task being executed twice.
@fl.workflow(
    on_failure=clean_up,
    failure_policy=WorkflowFailurePolicy.FAIL_AFTER_EXECUTABLE_NODES_COMPLETE,
)
def wf(name: str = "my_cluster"):
    c = create_cluster(name=name)
    t = t1(a=1, b="2")
    # d = delete_cluster(name=name)
    c >> t


if __name__ == "__main__":
    from flytekit.clis.sdk_in_container import pyflyte
    from click.testing import CliRunner
    import os

    runner = CliRunner()
    path = os.path.realpath(__file__)
    # result = runner.invoke(pyflyte.main, ["run", path, "wf"])
    # print("Local Execution: ", result.output)
    result = runner.invoke(pyflyte.main, ["run", "--remote", path, "wf"])
    print("Remote Execution: ", result.output)
