import datetime
from flytekit import FlyteRemote, WorkflowExecutionPhase
from flytekit.exceptions.user import FlyteTimeout
from flytekit.configuration import Config
from flytekit.clis.sdk_in_container import run


def run(path: str, workflow: str):
    from flytekit.clis.sdk_in_container import pyflyte
    from click.testing import CliRunner
    import os

    runner = CliRunner()
    # result = runner.invoke(pyflyte.main, ["run", path, "wf"])
    # print("Local Execution: ", result.output)
    result = runner.invoke(pyflyte.main, ["run", "--remote", path, workflow])
    print("Remote Execution: ", result.output)


# Trigger workflow using local code
# execution_id = run("workflow.py", "workflow")
# execution_id = run("branch_workflow.py", "wf")
# Observe it


ENDPOINT = "localhost:30080"
PROJECT = "flytesnacks"   # or "default"
DOMAIN = "development"
remote = FlyteRemote(
    config=Config.for_endpoint(endpoint=ENDPOINT, insecure=True),
    default_project=PROJECT,
    default_domain=DOMAIN,
)

wf = remote.fetch_workflow(name="branch_workflow.wf")
# wf = remote.fetch_workflow(name="failure_node_workflow.wf")
execution = remote.execute(wf, inputs={})
print(execution.id)
# execution = remote.fetch_execution(name=)
# Assert that it ran under 120 seconds. Wait throws an exception in case of timeout
try:
    execution = remote.wait(execution=execution, timeout=datetime.timedelta(minutes=2))
except FlyteTimeout as timeout:
    assert False, timeout

print("Execution Error:", execution.error)
assert (
    execution.closure.phase == WorkflowExecutionPhase.SUCCEEDED
), f"Execution failed with phase: {execution.closure.phase}"
