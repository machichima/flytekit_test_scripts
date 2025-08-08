# from pydantic import BaseModel
from typing import Literal
from flytekit import task, workflow, ImageSpec
from flytekit.loggers import logger


# class People(BaseModel):
#     name: Literal["a"]
#


image_spec = ImageSpec(
    base_image="ghcr.io/machichima/flytekit-private-image:toozpo4nnklfydlkhs8seg",
    name="flytekit-private-image",
    registry="localhost:30000",
    packages=["scikit-learn"],
    # registry="ghcr.io/machichima",
)


@task(cache=False, container_image=image_spec)
def get_name(name: Literal["a"]) -> str:
    return name

# @task(container_image=image_spec)
# def get_people_name(people: People) -> str:
#     return people.name


@workflow
def wf() -> str:
    return get_name("a")


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
