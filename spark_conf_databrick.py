from flytekit import task, workflow
from flytekitplugins.spark import DatabricksV2 as Databricks

task_conf = Databricks(
    # spark_conf={
    #     "spark.driver.memory": "1000M",
    #     "spark.executor.memory": "1000M",
    #     "spark.executor.cores": "1",
    #     "spark.executor.instances": "2",
    #     "spark.driver.cores": "1",
    # },
    databricks_conf={
        "run_name": "flyte agent basic worfkflow test",
        "new_cluster": {
            "apply_policy_default_values": "true",
            "aws_attributes": {
                "instance_profile_arn": "...",
                "availability": "ON_DEMAND",
            },
            "docker_image": {"url": "..."},
            "node_type_id": "m5.large",
            "num_workers": 1,
            "policy_id": "123",
        },
    },
)


@task(task_config=task_conf)
def example_task() -> str:
    return "hi"


@workflow
def wf() -> str:
    return example_task()


if __name__ == "__main__":
    from flytekit.clis.sdk_in_container import pyflyte
    from click.testing import CliRunner
    import os

    runner = CliRunner()
    path = os.path.realpath(__file__)
    result = runner.invoke(pyflyte.main, ["run", "--raw-output-data-prefix", "s3://my-s3-bucket/test/", path, "wf"])
    print("Local Execution: ", result.output)
    # result = runner.invoke(pyflyte.main, ["run", "--remote", path, "wf"])
    # print("Remote Execution: ", result.output)
