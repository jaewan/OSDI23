import json
import csv
import os
import resource
import time
from typing import List
import traceback

import numpy as np
import psutil

import ray
from ray._private.internal_api import memory_summary
from ray.data._internal.arrow_block import ArrowRow
from ray.data._internal.util import _check_pyarrow_version
from ray.data.block import Block, BlockMetadata
from ray.data.context import DatasetContext
from ray.data.datasource import Datasource, ReadTask

import sys
sys.path.insert(0, '/home/'+os.getlogin()+'/OSDI23/microbench/instantSubmission/')
from common import get_num_spilled_objs

class RandomIntRowDatasource(Datasource[ArrowRow]):
    """An example datasource that generates rows with random int64 columns.

    Examples:
        >>> source = RandomIntRowDatasource()
        >>> ray.data.read_datasource(source, n=10, num_columns=2).take()
        ... {'c_0': 1717767200176864416, 'c_1': 999657309586757214}
        ... {'c_0': 4983608804013926748, 'c_1': 1160140066899844087}
    """

    def prepare_read(
        self, parallelism: int, n: int, num_columns: int
    ) -> List[ReadTask]:
        _check_pyarrow_version()
        import pyarrow

        read_tasks: List[ReadTask] = []
        block_size = max(1, n // parallelism)

        def make_block(count: int, num_columns: int) -> Block:
            return pyarrow.Table.from_arrays(
                np.random.randint(
                    np.iinfo(np.int64).max, size=(num_columns, count), dtype=np.int64
                ),
                names=[f"c_{i}" for i in range(num_columns)],
            )

        schema = pyarrow.Table.from_pydict(
            {f"c_{i}": [0] for i in range(num_columns)}
        ).schema

        i = 0
        while i < n:
            count = min(block_size, n - i)
            meta = BlockMetadata(
                num_rows=count,
                size_bytes=8 * count * num_columns,
                schema=schema,
                input_files=None,
                exec_stats=None,
            )
            read_tasks.append(
                ReadTask(
                    lambda count=count, num_columns=num_columns: [
                        make_block(count, num_columns)
                    ],
                    meta,
                )
            )
            i += block_size

        return read_tasks

def store_results(memory_stats, res_str, result_path):
    num,size,migration_count = get_num_spilled_objs()
    print("Spilled Amount:", size)
    print("Migration Count:", migration_count)
    if 'dummy' in result_path:
        return

    # Get Spilled, Restored amount
    words = memory_stats.split()
    spilled_amount = ""
    spilled_objects = ""
    write_throughput = ""
    if "Spilled" in words:
        idx = words.index("Spilled")
        spilled_amount = words[idx+1]
        spilled_objects = words[idx+3]
        write_throughput = words[idx+8]

    restored_amount = ""
    restored_objects = ""
    read_throughput = ""
    if "Restored" in words:
        idx = words.index("Restored")
        restored_amount = words[idx+1]
        restored_objects = words[idx+3]
        read_throughput = words[idx+8]


    # Get Execution Time
    words = res_str[res_str.index("executed")+1:].split()
    idx = words.index("executed")
    runtime = words[idx+2]
    data = [runtime[:-1], spilled_amount, spilled_objects, write_throughput, restored_amount, restored_objects, read_throughput, migration_count]

    # Write the results as a csv file. The format is defined in run script
    with open(result_path, 'a', encoding='UTF-8', newline='') as f:
        writer = csv.writer(f)
        if 'config' in result_path:
            d = [OBJECT_STORE_SIZE, NUM_WORKERS, num_partitions, partition_size, data[0]]
            writer.writerow(d)
            return
        writer.writerow(data)


def boolean_string(s):
    if s not in {'False', 'True', 'false', 'true'}:
        raise ValueError('Not a valid boolean string')
    return (s == 'True' or  s == 'true')


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-partitions", help="number of partitions", default="50", type=str
    )
    parser.add_argument(
        "--partition-size",
        help="partition size (bytes)",
        default="200e6",
        type=str,
    )
    parser.add_argument(
        "--shuffle", help="shuffle instead of sort", action="store_true"
    )
    parser.add_argument("--use-polars", action="store_true")
    parser.add_argument('--RESULT_PATH', '-r', type=str, default="../data/dummy.csv")
    parser.add_argument('--NUM_WORKER', '-nw', type=int, default=16)
    parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=9_000_000_000)
    parser.add_argument('--MULTI_NODE', '-m', type=boolean_string, default=False)

    args = parser.parse_args()

    global NUM_WORKERS 
    global OBJECT_STORE_SIZE 
    global num_partitions
    global partition_size
    NUM_WORKERS = args.NUM_WORKER 
    OBJECT_STORE_SIZE = args.OBJECT_STORE_SIZE

    if args.use_polars and not args.shuffle:
        print("Using polars for sort")
        ctx = DatasetContext.get_current()
        ctx.use_polars = True

    result_path = args.RESULT_PATH
    spill_dir = os.getenv('RAY_SPILL_DIR')
    if args.MULTI_NODE or not spill_dir:
        ray.init()
        print("Ray default init")
    else:
        ray.init(_system_config={"object_spilling_config": json.dumps({"type": "filesystem",
                                    "params": {"directory_path": spill_dir}},)})#, num_cpus=NUM_WORKERS, object_store_memory=OBJECT_STORE_SIZE)
        print("Ray spill dir set")

    num_partitions = int(args.num_partitions)
    partition_size = int(float(args.partition_size))
    print(
        f"Dataset size: {num_partitions} partitions, "
        f"{partition_size / 1e9}GB partition size, "
        f"{num_partitions * partition_size / 1e9}GB total"
    )
    start_time = time.time()
    source = RandomIntRowDatasource()
    num_rows_per_partition = partition_size // 8
    ds = ray.data.read_datasource(
        source,
        parallelism=num_partitions,
        n=num_rows_per_partition * num_partitions,
        num_columns=1,
    )
    exc = None
    try:
        if args.shuffle:
            ds = ds.random_shuffle()
        else:
            ds = ds.sort(key="c_0")
    except Exception as e:
        exc = e
        pass

    end_time = time.time()

    duration = end_time - start_time
    print("Finished in", duration)
    print("")

    print("==== Driver memory summary ====")
    maxrss = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1e3)
    print(f"max: {maxrss / 1e9}/GB")
    process = psutil.Process(os.getpid())
    rss = int(process.memory_info().rss)
    print(f"rss: {rss / 1e9}/GB")

    memory_stats = ""
    try:
        memory_stats = memory_summary(stats_only=True)
        print(memory_stats)
    except Exception:
        print("Failed to retrieve memory summary")
        print(traceback.format_exc())
    print("")


    print(ds.stats())
    store_results(memory_stats, ds.stats(), result_path)

    if "TEST_OUTPUT_JSON" in os.environ:
        out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
        results = {
            "time": duration,
            "success": "1" if exc is None else "0",
            "num_partitions": num_partitions,
            "partition_size": partition_size,
            "perf_metrics": [
                {
                    "perf_metric_name": "peak_driver_memory",
                    "perf_metric_value": maxrss,
                    "perf_metric_type": "MEMORY",
                },
                {
                    "perf_metric_name": "runtime",
                    "perf_metric_value": duration,
                    "perf_metric_type": "LATENCY",
                },
            ],
        }
        json.dump(results, out_file)

    if exc:
        raise exc
