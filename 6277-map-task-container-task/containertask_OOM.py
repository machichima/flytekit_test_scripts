# from flytekit import ContainerTask
from flytekit.image_spec.image_spec import ImageSpec
from flytekit import ContainerTask, ImageSpec, kwtypes, task, workflow, map_task

from uuid import uuid4

image_version = str(uuid4())[:8]
# tag_format=f"{{spec_hash}}-{uuid4().hex[:8]}"

container_task = ContainerTask(
    name="containertask-oom",
    cache_version="0.0.0",
    input_data_dir="/var/inputs",
    output_data_dir="/var/outputs",
    # inputs=kwtypes(data_point=int),
    outputs=kwtypes(res=bool),
    image="debian:bookworm-slim",
    # command=["/bin/sh", "-c", "head", "-c", "1G", "/dev/zero", "|", "tail"],
    # command=["/bin/sh", "-c", "head -c 1G /dev/zero | tail"]
    command=[
        "/bin/sh",
        "-c",
        # "echo Input value: $(cat {{.inputs.data_point}}) && head -c 1G /dev/zero | tail"
        "echo true > /var/outputs/res && head -c 1G /dev/zero | tail"
    ],
)


@workflow
def wf():
    return container_task()


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

    result = runner.invoke(
        pyflyte.main,
        ["run", "--remote", path, "wf"],
    )
    # result = runner.invoke(pyflyte.main, ["run", "--remote", path, "detect_anomalies"])
    print("Remote Execution: ", result.output)
