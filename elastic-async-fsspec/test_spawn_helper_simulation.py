#!/usr/bin/env python3
"""
Simulate the exact spawn_helper behavior to reproduce the issue
"""

import os
import sys
import tempfile
import multiprocessing
import threading
import traceback
from pathlib import Path

# Add flytekit to path  
sys.path.insert(0, '/home/nary/workData/open-source/flyte/flytekit')

import cloudpickle
from flytekit import FlyteFile


def my_task_function(dataset: FlyteFile):
    """Exact same function as torch_flytefile_multiprocess.py"""
    print(f"Task function PID: {os.getpid()}, Thread: {threading.current_thread().name}")
    print(f"Dataset path: {dataset.path}")
    
    # This is the problematic call
    path = dataset.download()
    print(f"Downloaded to: {path}")
    
    file_exists = Path(path).is_file()
    print(f"File exists: {file_exists}")
    
    return file_exists


def spawn_helper_simulation(serialized_func: bytes, raw_output_prefix: str, dataset_path: str):
    """
    Simulate the exact spawn_helper behavior from Elastic
    """
    print(f"\n=== Spawn Helper Simulation PID: {os.getpid()} ===")
    print(f"Thread: {threading.current_thread().name}")
    print(f"Raw output prefix: {raw_output_prefix}")
    
    try:
        # Import here to avoid issues
        from flytekit.bin.entrypoint import setup_execution
        
        print("Setting up execution context...")
        # This is exactly what spawn_helper does
        with setup_execution(
            raw_output_data_prefix=raw_output_prefix,
            checkpoint_path=None,
            prev_checkpoint=None,
        ) as ctx:
            print("Execution context setup complete")
            print(f"Context: {type(ctx)}")
            
            # Deserialize function
            print("Deserializing function...")
            fn = cloudpickle.loads(serialized_func)
            print("Function deserialized")
            
            # Create FlyteFile (this should use the setup context)
            print("Creating FlyteFile...")
            dataset = FlyteFile(path=dataset_path)
            print(f"FlyteFile created: {dataset.path}")
            
            # Execute function - this is where the issue occurs
            print("Executing task function...")
            result = fn(dataset)
            print(f"Task function result: {result}")
            
            return result
            
    except Exception as e:
        print(f"spawn_helper_simulation failed: {e}")
        traceback.print_exc()
        return False


def test_spawn_helper_behavior():
    """Test the complete spawn helper behavior"""
    print("Testing spawn_helper behavior simulation")
    print(f"Main PID: {os.getpid()}")
    
    try:
        # Serialize the task function
        serialized_func = cloudpickle.dumps(my_task_function)
        print(f"Function serialized: {len(serialized_func)} bytes")
        
        # Prepare arguments like real spawn_helper
        raw_output_prefix = f"file://{tempfile.mkdtemp()}/raw_output"  
        dataset_path = "s3://my-s3-bucket/test/test.txt"
        
        print(f"Using raw_output_prefix: {raw_output_prefix}")
        print(f"Using dataset_path: {dataset_path}")
        
        # Test in spawn process
        ctx = multiprocessing.get_context('spawn')
        with ctx.Pool(1) as pool:
            result = pool.apply(
                spawn_helper_simulation, 
                (serialized_func, raw_output_prefix, dataset_path)
            )
        
        print(f"Final result: {result}")
        
        if not result:
            print("BUG REPRODUCED: spawn_helper simulation failed!")
        else:
            print("spawn_helper simulation succeeded")
            
    except Exception as e:
        print(f"Test failed: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    test_spawn_helper_behavior()