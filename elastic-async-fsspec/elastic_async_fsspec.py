#!/usr/bin/env python3
"""
Simple test script to reproduce FlyteFile.download() failure in spawned processes.
Based on torch_flytefile_multiprocess.py issue.
"""

import os
import sys
import multiprocessing
import threading
import traceback
from pathlib import Path

# Add flytekit to the path
sys.path.insert(0, '/home/nary/workData/open-source/flyte/flytekit')

import cloudpickle
from flytekit import FlyteFile

# Same S3 path as in torch_flytefile_multiprocess.py
DEFAULT_REMOTE_PATH = "s3://my-s3-bucket/test/test.txt"


def test_flytefile_main_thread():
    """Test FlyteFile.download() in main thread"""
    print(f"\n=== Main Thread Test ===")
    print(f"PID: {os.getpid()}, Thread: {threading.current_thread().name}")
    
    try:
        flyte_file = FlyteFile(path=DEFAULT_REMOTE_PATH)
        print(f"FlyteFile path: {flyte_file.path}")
        
        downloaded_path = flyte_file.download()
        print(f"Downloaded to: {downloaded_path}")
        
        # This is the assertion that fails in torch_flytefile_multiprocess.py
        if Path(downloaded_path).is_file():
            print(f"Main thread: File exists at {downloaded_path}")
            return True
        else:
            print(f"Main thread: File NOT found at {downloaded_path}")
            return False
            
    except Exception as e:
        print(f"✗ Main thread failed: {e}")
        traceback.print_exc()
        return False


def worker_function_fork(worker_id: int):
    """Worker function for fork method"""
    print(f"\n=== Fork Worker {worker_id} ===")
    print(f"PID: {os.getpid()}, Thread: {threading.current_thread().name}")
    
    try:
        flyte_file = FlyteFile(path=DEFAULT_REMOTE_PATH)
        downloaded_path = flyte_file.download()
        print(f"Fork Worker {worker_id}: Downloaded to {downloaded_path}")
        
        if Path(downloaded_path).is_file():
            print(f"Fork Worker {worker_id}: File exists")
            return True
        else:
            print(f"Fork Worker {worker_id}: File NOT found")
            return False
            
    except Exception as e:
        print(f"Fork Worker {worker_id} failed: {e}")
        traceback.print_exc()
        return False


def worker_function_spawn(worker_id: int):
    """Worker function for spawn method - this should reproduce the bug"""
    print(f"\n=== Spawn Worker {worker_id} ===")
    print(f"PID: {os.getpid()}, Thread: {threading.current_thread().name}")
    
    try:
        # Exactly like in torch_flytefile_multiprocess.py
        flyte_file = FlyteFile(path=DEFAULT_REMOTE_PATH)
        downloaded_path = flyte_file.download()
        print(f"Spawn Worker {worker_id}: Downloaded to {downloaded_path}")
        
        # This is where the bug occurs - file is not actually downloaded
        if Path(downloaded_path).is_file():
            print(f"Spawn Worker {worker_id}: File exists")
            return True
        else:
            print(f"Spawn Worker {worker_id}: File NOT found (BUG REPRODUCED)")
            return False
            
    except Exception as e:
        print(f"Spawn Worker {worker_id} failed: {e}")
        traceback.print_exc()
        return False


def spawn_helper(serialized_func: bytes, args: tuple):
    """Cloudpickle spawn helper - mimics Elastic behavior"""
    print(f"\n=== Spawn Helper ===")
    print(f"PID: {os.getpid()}, Thread: {threading.current_thread().name}")
    
    try:
        func = cloudpickle.loads(serialized_func)
        return func(*args)
    except Exception as e:
        print(f"Spawn helper failed: {e}")
        traceback.print_exc()
        return False


def main():
    print("Testing FlyteFile.download() with different multiprocessing methods")
    print(f"S3 path: {DEFAULT_REMOTE_PATH}")
    print(f"Main PID: {os.getpid()}")
    
    results = []
    
    # Test 1: Main thread (baseline)
    print("\n" + "="*50)
    print("Test 1: Main Thread")
    print("="*50)
    main_success = test_flytefile_main_thread()
    results.append(("Main thread", main_success))
    
    # Test 2: Fork method
    print("\n" + "="*50)
    print("Test 2: Fork Method")
    print("="*50)
    ctx_fork = multiprocessing.get_context('fork')
    with ctx_fork.Pool(2) as pool:
        fork_results = pool.map(worker_function_fork, [0, 1])
    fork_success = all(fork_results)
    results.append(("Fork method", fork_success))
    print(f"Fork results: {fork_results}")
    
    # Test 3: Spawn method (should reproduce bug)
    print("\n" + "="*50)
    print("Test 3: Spawn Method")
    print("="*50)
    ctx_spawn = multiprocessing.get_context('spawn')
    with ctx_spawn.Pool(2) as pool:
        spawn_results = pool.map(worker_function_spawn, [0, 1])
    spawn_success = all(spawn_results)
    results.append(("Spawn method", spawn_success))
    print(f"Spawn results: {spawn_results}")
    
    # Test 4: Spawn with cloudpickle (exact Elastic simulation)
    print("\n" + "="*50)
    print("Test 4: Spawn with Cloudpickle (Elastic)")
    print("="*50)
    serialized_func = cloudpickle.dumps(worker_function_spawn)
    with ctx_spawn.Pool(2) as pool:
        cloudpickle_results = [
            pool.apply(spawn_helper, (serialized_func, (i,))) 
            for i in [0, 1]
        ]
    cloudpickle_success = all(cloudpickle_results)
    results.append(("Spawn+cloudpickle", cloudpickle_success))
    print(f"Cloudpickle results: {cloudpickle_results}")
    
    # Summary
    print("\n" + "="*50)
    print("RESULTS SUMMARY")
    print("="*50)
    for test_name, success in results:
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{test_name:20} : {status}")
    
    failed_tests = [name for name, success in results if not success]
    if failed_tests:
        print(f"\n⚠️  Failed tests: {failed_tests}")
        print("This reproduces the torch_flytefile_multiprocess.py bug:")
        print("assert Path(path).is_file() fails because download() doesn't work in spawn")


if __name__ == "__main__":
    main()
