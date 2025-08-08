import flytekit as fl


@fl.task
def some_test():
    print("some test")


@fl.task
def return_index(character: str) -> int:
    print(f"In return index: {character}")
    if character.islower():
        return ord(character) - ord("a")
    else:
        return ord(character) - ord("A")


@fl.task
def update_list(freq_list: list[int], list_index: int) -> list[int]:
    print(f"In update list: {freq_list}")
    freq_list[list_index] += 1
    return freq_list


@fl.task
def derive_count(freq1: list[int], freq2: list[int]) -> int:
    print(f"in derive_count: {freq1}, {freq2}")
    count = 0
    for i in range(26):
        count += min(freq1[i], freq2[i])
    return count


@fl.dynamic
def count_characters(s1: str, s2: str) -> int:
    # s1 and s2 should be accessible
    print("start dynamic workflow")
    some_test()

    # Initialize empty lists with 26 slots each, corresponding to every alphabet (lower and upper case)
    freq1 = [0] * 26
    freq2 = [0] * 26

    # Loop through characters in s1
    for i in range(len(s1)):
        # Calculate the index for the current character in the alphabet
        index = return_index(character=s1[i])
        # Update the frequency list for s1
        freq1 = update_list(freq_list=freq1, list_index=index)
        # index and freq1 are not accessible as they are promises

    # looping through the string s2
    for i in range(len(s2)):
        # Calculate the index for the current character in the alphabet
        index = return_index(character=s2[i])
        # Update the frequency list for s2
        freq2 = update_list(freq_list=freq2, list_index=index)
        # index and freq2 are not accessible as they are promises

    # Count the common characters between s1 and s2
    print("last task")
    return derive_count(freq1=freq1, freq2=freq2)


@fl.dynamic
def start_count_characters(s1: str, s2: str) -> int:
    return count_characters(s1, s2)


@fl.workflow
def wf(s1: str = "s1", s2: str = "s2") -> int:
    return start_count_characters(s1=s1, s2=s2)


if __name__ == "__main__":
    import os

    from click.testing import CliRunner

    from flytekit.clis.sdk_in_container import pyflyte

    runner = CliRunner()
    path = os.path.realpath(__file__)
    # result = runner.invoke(pyflyte.main, ["run", path, "wf"])
    # print("Local Execution: ", result.output)
    result = runner.invoke(pyflyte.main, ["run", "--remote", path, "wf"])
    print("Remote Execution: ", result.output)
