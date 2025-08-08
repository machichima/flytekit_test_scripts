from pydantic import BaseModel

from flytekit import map_task
from typing import  List
from flytekit import task, workflow
from flytekit.image_spec.image_spec import ImageSpec

new_flytekit = "git+https://github.com/flyteorg/flytekit.git@c90a2ff747220c54cd3de554452eda3fe887fbce"
image_spec = ImageSpec(
    registry="localhost:30000",
    packages=[new_flytekit, "pandas", "pyarrow", "pydantic", "decorator"],
    apt_packages=["git"],
)


class MyBaseModel(BaseModel):
    my_floats: List[float] = [1.0, 2.0, 5.0, 10.0]

@task(container_image = image_spec)
def print_float(my_float: float):
    print(f"my_float: {my_float}")

@workflow
def wf(bm: MyBaseModel = MyBaseModel()):
    map_task(print_float)(my_float=bm.my_floats)

if __name__ == "__main__":
    wf()
