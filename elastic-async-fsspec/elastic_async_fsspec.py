#!/usr/bin/env python3
"""
Direct test of Elastic task execution to reproduce FlyteFile.download() failure.
This directly runs the same code path as @task(task_config=Elastic(...)).
"""

import os
import sys
import tempfile
import traceback
from pathlib import Path

# Add flytekit to the path
sys.path.insert(0, '/home/nary/workData/open-source/flyte/flytekit')

from flytekit import FlyteFile
from flytekit.core.context_manager import FlyteContextManager, FlyteContext
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.configuration import DataConfig
from flytekitplugins.kfpytorch.task import Elastic, PytorchElasticFunctionTask

# Same S3 path as in torch_flytefile_multiprocess.py
DEFAULT_REMOTE_PATH = "s3://my-s3-bucket/test/test.txt"


def my_task_function(dataset: FlyteFile):
    """
    Exact same function as in torch_flytefile_multiprocess.py
    """
    print(f"Worker PID: {os.getpid()}")
    print(f"Dataset path: {dataset.path}")
    
    # This is the call that fails in spawned processes
    path = dataset.download()
    print(f"Downloaded to: {path}")
    
    # This assertion fails in torch_flytefile_multiprocess.py
    file_exists = Path(path).is_file()
    print(f"File exists: {file_exists}")
    
    if not file_exists:
        print(f"BUG: File was not downloaded at {path}")
        return False
    else:
        print(f"Success: File downloaded")
        return True


def test_elastic_task():
    """Test Elastic task execution directly"""
    print(f"\n=== Testing Elastic Task ===")
    
    # Same config as torch_flytefile_multiprocess.py
    elastic_config = Elastic(nnodes=1, nproc_per_node=4)
    
    elastic_task = PytorchElasticFunctionTask(
        task_config=elastic_config,
        task_function=my_task_function
    )
    
    try:
        # Direct FlyteFile creation
        dataset = FlyteFile(path=DEFAULT_REMOTE_PATH)
        print(f"Created FlyteFile: {dataset.path}")
        
        # Execute the Elastic task
        result = elastic_task.execute(dataset=dataset)
        print(f"Elastic result: {result}")
        return result
        
    except Exception as e:
        print(f"Elastic task failed: {e}")
        traceback.print_exc()
        return False


def test_direct_function():
    """Test function directly without Elastic"""
    print(f"\n=== Testing Direct Function ===")
    
    try:
        dataset = FlyteFile(path=DEFAULT_REMOTE_PATH)
        result = my_task_function(dataset)
        print(f"Direct result: {result}")
        return result
        
    except Exception as e:
        print(f"Direct function failed: {e}")
        traceback.print_exc()
        return False


def main():
    print("Testing FlyteFile.download() with Elastic")
    print(f"S3 path: {DEFAULT_REMOTE_PATH}")
    print(f"Main PID: {os.getpid()}")
    
    results = []
    
    # Test 1: Direct function call (baseline)
    direct_success = test_direct_function()
    results.append(("Direct function", direct_success))
    
    # Test 2: Elastic task (this should reproduce the bug)
    elastic_success = test_elastic_task()
    results.append(("Elastic task", elastic_success))
    
    # Summary
    print("\n" + "="*50)
    print("RESULTS SUMMARY")
    print("="*50)
    for test_name, success in results:
        status = "PASS" if success else "FAIL"
        print(f"{test_name:15} : {status}")
    
    failed_tests = [name for name, success in results if not success]
    if failed_tests:
        print(f"\nFailed tests: {failed_tests}")
        print("This reproduces the torch_flytefile_multiprocess.py bug")
    else:
        print("All tests passed")


if __name__ == "__main__":
    main()
