import os
import sys


def write_output(output_dir, output_file, v):
    print(output_dir, output_file)
    print("res: ", str(v))
    with open(f"{output_dir}/{output_file}", "w") as f:
        f.write(str(v))


threshold = 11


def detect_anomalies(data_point: int) -> bool:
    return data_point > threshold


def calculate_area(a, b):
    return math.pi * a * b


def main(data_point, output_dir):
    # parse list
    # li = data_point.strip('][').split(',')
    # sep_data_point = [eval(i) for i in li][0]
    print(data_point)
    # print(li)

    # get the job index
    # index = int(os.environ.get(os.environ.get("BATCH_JOB_ARRAY_INDEX_VAR_NAME", "1")))
    # print(f"index: {index}")

    res = detect_anomalies(eval(data_point))

    write_output(output_dir, "res", res)
    write_output(output_dir, "_ERROR", "")


if __name__ == "__main__":
    data_point = sys.argv[1]
    output_dir = sys.argv[2]

    main(data_point, output_dir)
