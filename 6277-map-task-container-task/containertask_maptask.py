# from flytekit import ContainerTask
from flytekit.image_spec.image_spec import ImageSpec
from flytekit import ContainerTask, ImageSpec, kwtypes, task, workflow, map_task

from uuid import uuid4
image_version = str(uuid4())[:8]
# tag_format=f"{{spec_hash}}-{uuid4().hex[:8]}"

container_task = ContainerTask(
    name="square-container-task",
    cache_version="0.0.14",
    input_data_dir="/var/inputs",
    output_data_dir="/var/outputs",
    inputs=kwtypes(data_point=int),
    outputs=kwtypes(res=bool),
    image=ImageSpec(
        name="contianer_task_anomalies",
        base_image="python:3.10",
        # tag_format=tag_format,
        registry="localhost:30000",
        builder="default",
        copy=["detect_anomalies.py"],
    ),
    command=[
        "python",
        "detect_anomalies.py",
        "{{.inputs.data_point}}",
        "/var/outputs",
    ],
)


@workflow
def detect_one_anomaly(data_point: int) -> bool:
    return container_task(data_point=data_point)


@workflow
def detect_anomalies(data: list[int] = [10, 12, 11, 10]) -> list[bool]:
    # res = container_task(data_point=1)
    # print(res)
    return map_task(container_task)(data_point=data)

# @workflow
# def wf(data: list[int] = [10, 12, 11, 10]) -> list[bool]:
#     # Use the map task to apply the anomaly detection function to each data point
#     return map_task(detect_anomalies)(data_point=data)


if __name__ == "__main__":
    from flytekit.clis.sdk_in_container import pyflyte
    from click.testing import CliRunner
    import os

    runner = CliRunner()
    path = os.path.realpath(__file__)
    # result = runner.invoke(pyflyte.main, ["run", path, "detect_one_anomaly", "--data_point", "1"])
    # print("Local Execution: ", result.output)
    # result = runner.invoke(pyflyte.main, ["run", path, "detect_anomalies"])
    # print("Local Execution: ", result.output)

    result = runner.invoke(pyflyte.main, ["run", "--remote", path, "detect_one_anomaly", "--data_point", "1"])
    # result = runner.invoke(pyflyte.main, ["run", "--remote", path, "detect_anomalies"])
    print("Remote Execution: ", result.output)
