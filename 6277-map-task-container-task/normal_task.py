from flytekit import task, workflow

threshold = 11


# @task
# def detect_anomalies(data_point: int) -> tuple[bool, str]:
#     return data_point > threshold, "hi"

@task
def detect_anomalies(data_point: int) -> bool:
    return True


# @workflow
# def wf() -> tuple[bool, str]:
#     return detect_anomalies(1)

@workflow
def wf() -> bool:
    return detect_anomalies(1)


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
