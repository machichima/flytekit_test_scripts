from flytekit import map_task, task, workflow

threshold = 11


@task
def detect_anomalies(data_point: int) -> bool:
    return data_point > threshold


@workflow
def wf(data: list[int] = [10, 12, 11, 10]) -> list[bool]:
    # Use the map task to apply the anomaly detection function to each data point
    return map_task(detect_anomalies)(data_point=data)


if __name__ == "__main__":
    from flytekit.clis.sdk_in_container import pyflyte
    from click.testing import CliRunner
    import os

    runner = CliRunner()
    path = os.path.realpath(__file__)
    result = runner.invoke(pyflyte.main, ["run", path, "wf"])
    print("Local Execution: ", result.output)
    # result = runner.invoke(pyflyte.main, ["run", "--remote", path, "wf"])
    # print("Remote Execution: ", result.output)
