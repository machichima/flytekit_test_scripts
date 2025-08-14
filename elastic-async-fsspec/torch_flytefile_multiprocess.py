from pathlib import Path
import time

from flytekit import task, workflow, FlyteFile, ImageSpec, Resources
from flytekitplugins.kfpytorch.task import Elastic, PyTorch


DEFAULT_REMOTE_PATH = "s3://my-s3-bucket/test/test.txt"

image_spec = ImageSpec(
    registry="localhost:30000",
    packages=[
        "flytekitplugins-kfpytorch[elastic]",
        "numpy",
        "torch",
    ],
)


@task(
    task_config=Elastic(nnodes=1, nproc_per_node=4),
    container_image=image_spec,
    # shared_memory=True,
    requests=Resources(cpu="4", mem="4Gi"),
    limits=Resources(cpu="4", mem="4Gi"),
)
def my_task(dataset: FlyteFile):
    # import torch.distributed as dist
    # import time
    #
    # dist.init_process_group(backend="gloo")
    # rank = dist.get_rank()
    #
    # # All ranks attempt download, but rank 0 goes first
    # if rank == 0:
    #     path = dataset.download()
    #     # Wait for actual download completion
    #     while not Path(path).is_file():
    #         time.sleep(0.1)
    #
    # # Sync point - rank 0 finished downloading
    # dist.barrier()

    # Now all ranks can safely call download()
    # (should return quickly since file exists)
    rank = 0
    path = dataset.download()
    print(f"rank - path: {rank} - {path}", flush=True)
    # time.sleep(10000)

    assert Path(path).is_file()  # fail


# def my_task(dataset: FlyteFile):
#     # path = dataset.download()
#     path = dataset.path
#     print(f"path: {path}")
#     # .download() immediately returns and file is not there
#     assert Path(path).is_file()  # this will raise


@workflow
def wf():
    # dataset = FlyteFile(path=DEFAULT_REMOTE_PATH)
    # path = dataset.download()
    # file = FlyteFile(path=path)

    file = FlyteFile(path=DEFAULT_REMOTE_PATH)

    outputs = my_task(dataset=file)


if __name__ == "__main__":
    import os

    from click.testing import CliRunner

    from flytekit.clis.sdk_in_container import pyflyte

    runner = CliRunner()
    path = os.path.realpath(__file__)
    result = runner.invoke(pyflyte.main, ["run", path, "wf"])
    print("Local Execution: ", result.output)
    # result = runner.invoke(pyflyte.main, ["run", "--remote", path, "wf"])
    # print("Remote Execution: ", result.output)
