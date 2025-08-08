# from flytekit import ContainerTask
import functools

from flytekit import ContainerTask, kwtypes, map_task, workflow
calculate_ellipse_area_python_template_style = ContainerTask(
    name="calculate_ellipse_area_python_template_style",
    input_data_dir="/var/inputs",
    output_data_dir="/var/outputs",
    inputs=kwtypes(a=float, b=float),
    outputs=kwtypes(area=float),
    image="ghcr.io/flyteorg/rawcontainers-python:v2",
    command=[
        "python",
        "calculate-ellipse-area.py",
        "{{.inputs.a}}",
        "{{.inputs.b}}",
        "/var/outputs",
    ],
)


@workflow
def wf(a: float = 0.5, b: float = 0.4) -> float:
    res = calculate_ellipse_area_python_template_style(a, b)
    return res


# @workflow
# def wf(a: list[float] = [3.0, 4.0, 5.0], b: float = 4.0) -> list[float]:
#     partial_task = functools.partial(calculate_ellipse_area_python_template_style, b=b)
#     res = map_task(partial_task)(a=a)
#     return res
#

if __name__ == "__main__":
    import os

    from click.testing import CliRunner

    from flytekit.clis.sdk_in_container import pyflyte

    runner = CliRunner()
    path = os.path.realpath(__file__)
    # result = runner.invoke(pyflyte.main, ["run", path, "wf"])
    # result = runner.invoke(pyflyte.main, ["run", path, "nomap_wf"])
    # print("Local Execution: ", result.output)
    # result = runner.invoke(pyflyte.main, ["run", path, "wf"])
    # print("Local Execution: ", result.output)

    result = runner.invoke(pyflyte.main, ["run", "--remote", path, "nomap_wf"])
    # result = runner.invoke(pyflyte.main, ["run", "--remote", path, "wf"])
    print("Remote Execution: ", result.output)

    # result = runner.invoke(pyflyte.main, ["run", "--remote", path, "detect_one_anomaly", "--data_point", "1"])
    # result = runner.invoke(pyflyte.main, ["run", "--remote", path, "wf"])
    # print("Remote Execution: ", result.output)
