#!/usr/bin/env python3
"""
Simple comparison: direct get_data call vs functools.partial in spawn
"""

import os
import sys
import multiprocessing
import asyncio
from functools import partial

sys.path.insert(0, "/home/nary/workData/open-source/flyte/flytekit")

from flytekit.core.context_manager import FlyteContextManager


def test_both_methods(partial_func):
    """Test both direct call and partial call in spawn process"""
    print(f"Spawn process PID: {os.getpid()}")

    # Test 1: Direct call
    print("\n--- TEST 1: Direct Call ---")
    current_ctx = FlyteContextManager.current_context()
    current_file_access = current_ctx.file_access

    try:
        result1 = current_ctx.file_access.get_data(
            remote_path="s3://my-s3-bucket/test/test.txt",
            local_path="/tmp/test_direct.txt",
        )
        is_coro1 = asyncio.iscoroutine(result1)
        print(f"Direct call result type: {type(result1)}")
        print(f"Direct call is coroutine: {is_coro1}")
        if is_coro1:
            result1.close()
    except Exception as e:
        print(f"Direct call error: {e}")
        is_coro1 = None

    # Test 2: Partial call
    print("\n--- TEST 2: Partial Call ctx from main ---")
    try:
        result2 = partial_func()
        is_coro2 = asyncio.iscoroutine(result2)
        print(f"Partial function referece: {partial_func.func.__self__}")
        print(f"Partial function use file access in current process?: {partial_func.func.__self__ is current_file_access}")
        print(f"Partial call result type: {type(result2)}")
        print(f"Partial call is coroutine: {is_coro2}")
        if is_coro2:
            result2.close()
    except Exception as e:
        print(f"Partial call error: {e}")
        is_coro2 = None

    print("\n--- TEST 3: Partial Call ctx from spawn ---")
    try:
        partial_func = partial(
            current_ctx.file_access.get_data,
            remote_path="s3://my-s3-bucket/test/test.txt",
            local_path="/tmp/test_partial.txt",
        )
        result3 = partial_func()
        is_coro3 = asyncio.iscoroutine(result3)
        print(f"Partial function referece: {partial_func.func.__self__}")
        print(f"Partial function use file access in current process?: {partial_func.func.__self__ is current_file_access}")
        print(f"Partial call result type: {type(result3)}")
        print(f"Partial call is coroutine: {is_coro3}")
        if is_coro3:
            result3.close()
    except Exception as e:
        print(f"Partial call error: {e}")
        is_coro3 = None

    # Summary
    print("\n--- SUMMARY ---")
    print(f"Direct call returns coroutine: {is_coro1}")
    print(f"Partial call (ctx from main) returns coroutine: {is_coro2}")
    print(f"Partial call (ctx from spawn) returns coroutine: {is_coro3}")

    return {
        "direct_is_coro": is_coro1,
        "partial_main_ctx_is_coro": is_coro2,
        "partial_spawn_ctx_is_coro": is_coro3,
    }


def main():
    """Main function"""
    print("Creating partial in main process...")
    main_ctx = FlyteContextManager.current_context()
    partial_func = partial(
        main_ctx.file_access.get_data,
        remote_path="s3://my-s3-bucket/test/test.txt",
        local_path="/tmp/test_partial.txt",
    )

    print("Testing in spawn process...")
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(1) as pool:
        result = pool.apply(test_both_methods, (partial_func,))

    print(f"\nFinal result: {result}")

    # Check hypothesis
    if (
        result["direct_is_coro"] == False
        and result["partial_main_ctx_is_coro"] == True
        and result["partial_spawn_ctx_is_coro"] == False
    ):
        print("\nFinal result:")
        print("- Direct call: NOT a coroutine (works)")
        print("- Partial call main ctx: IS a coroutine (fails)")
        print("- Partial call spawn ctx: NOT a coroutine (works)")
    else:
        print(f"\nUnexpected result: {result}")


if __name__ == "__main__":
    main()

