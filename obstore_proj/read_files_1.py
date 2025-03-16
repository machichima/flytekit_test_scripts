import os

from flytekit import task
from flytekit.types.file import FlyteFile


# Remote path
remote_path = "s3://my-s3-bucket/test.json"


@task()
def task_read_and_shuffle_file(input_file: FlyteFile) -> FlyteFile:
    """
    Reads the input file as a DataFrame, shuffles the rows, and writes the shuffled DataFrame to a new file.
    """
    input_file.download()
    df = pd.read_csv(input_file.path)

    # Shuffle the DataFrame rows
    shuffled_df = df.sample(frac=1).reset_index(drop=True)

    output_file_path = "data_shuffle.csv"
    shuffled_df.to_csv(output_file_path, index=False)

    return FlyteFile(output_file_path)


@task()
def task_remove_column(input_file: FlyteFile, column_name: str) -> FlyteFile:
    """
    Reads the input file as a DataFrame, removes a specified column, and outputs it as a new file.
    """
    input_file.download()
    df = pd.read_csv(input_file.path)

    # remove column
    if column_name in df.columns:
        df = df.drop(columns=[column_name])

    output_file_path = "data_finished.csv"
    df.to_csv(output_file_path, index=False)

    return FlyteFile(output_file_path)

@workflow
def wf() -> FlyteFile:
    existed_file = FlyteFile("s3://custom-bucket/data.csv")
    shuffled_file = task_read_and_shuffle_file(input_file=existed_file)
    result_file = task_remove_column(input_file=shuffled_file, column_name="County")
    return result_file

if __name__ == "__main__":
    from flytekit.clis.sdk_in_container import pyflyte
    from click.testing import CliRunner

    runner = CliRunner()
    path = os.path.realpath(__file__)
    result = runner.invoke(pyflyte.main, ["run", path, "create_ff"])
    print(result.output)
