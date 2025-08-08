import flytekit as fl
import pandas as pd


image_spec = fl.ImageSpec(
    registry="localhost:30000",
    packages=[
        "pandas", "pyarrow", "fastparquet"
    ],
)


# @fl.task(container_image=image_spec)
# def load_model(name: str) -> pd.DataFrame:
#     return pd.DataFrame({name: [1, 2, 3, 4, 5]})


@fl.task(container_image=image_spec)
def predict_df(model: pd.DataFrame, n: int):
    print(model)
    print(n)


@fl.workflow
def predict_wf(n: int, model: pd.DataFrame):
    predict_df(model=model, n=n)


@fl.dynamic()
def conc_prediction(input: pd.DataFrame):
    for n in range(1, 15):
        predict_wf(model=input, n=n)


@fl.workflow
def wf():
    # output = load_model(name="foo")
    conc_prediction(input=pd.DataFrame({"name": [1, 2, 3, 4, 5]}))


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
