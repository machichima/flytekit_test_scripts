# from pydantic import BaseModel
from typing import Literal
from flytekit import task, workflow, ImageSpec
from flytekit.loggers import logger


# class People(BaseModel):
#     name: Literal["a"]
#

# new_flytekit = "git+https://github.com/BarryWu0812/flytekit.git@2630515dea05eafb422767c1e40c00ac4105f176"
image_spec = ImageSpec(
    registry="localhost:30000",
    # packages=[
    #     new_flytekit,
    # ],
    apt_packages=["git"],
) 
# image_spec = ImageSpec(
#     registry="localhost:30000",
#     packages=["pydantic"],
# )


@task(cache=False)
def get_name(name: Literal["b"]) -> str:
    return name

# @task(container_image=image_spec)
# def get_people_name(people: People) -> str:
#     return people.name


@workflow
def wf() -> str:
    return get_name("b")


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
