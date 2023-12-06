from typing import  Dict, List, Tuple, Any
import time
import os
import pandas as pd
import numpy as np

from sklearn.base import BaseEstimator
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
import mmd_utils as mmt


from pyarrow import parquet as pq
import pyarrow.compute as pc
import ray

OBJECT_SIZE = 10_000_000

@ray.remote
def transform_and_split(
    pickup_location_id_and_data: Tuple[int, pd.DataFrame],
    inflate_data:np.ndarray,
    ):
    # Perform transformation
    start_time = time.time()
    # The underlying numpy arrays are stored in the Ray object
    # store for efficient access, making them immutable. We therefore
    # copy the DataFrame to obtain a mutable copy we can transform.
    pickup_location_id, data = pickup_location_id_and_data
    data = data.copy()
    data = mmt.transform_batch(data)
    try:
        train, test = train_test_split(data)
        transform_time = time.time() - start_time

        return (train,test)
    except Exception:
        return (data,data)


# Redefine the train_and_evaluate task to use in-memory data.
# We still keep file_name and pickup_location_id for identification purposes.
@ray.remote
def train_and_evaluate_optimized(
    pickup_location_id_and_data: Tuple[int, pd.DataFrame],
    file_name: str,
    models: List[BaseEstimator],
    inflate_data:np.ndarray,
    verbose: bool = False,
) -> Tuple[str, str, List[Tuple[BaseEstimator, float]]]:
    """
    An optimized version of this function that uses in-memory data.

    Args:
        pickup_location_id_and_data: A tuple of id and its respective Pandas
        DataFrame
        file_name: file used to extract id's dataframe
        models: list of sklearn estimators to train on
        verbose: boolean, default False to print stats and times
    
    Returns:
        A tuple file_name, id, and list tuple of estimators and respective MAE
    """
    # Unpack the tupel to extract respective id and data
    pickup_location_id, data = pickup_location_id_and_data

    # Perform transformation
    start_time = time.time()
    # The underlying numpy arrays are stored in the Ray object
    # store for efficient access, making them immutable. We therefore
    # copy the DataFrame to obtain a mutable copy we can transform.
    data = data.copy()
    data = mmt.transform_batch(data)
    transform_time = time.time() - start_time
    if verbose:
        mmt.print_time_info_stats(
            f"Data transform time for LocID: {pickup_location_id}: {transform_time}"
    )

    return (
        file_name,
        pickup_location_id,
        mmt.train_and_evaluate_internal(data, models, pickup_location_id),
    )

@ray.remote(num_returns=2)
def get_locdf_and_ids(file:str):
    try:
        locdf = pq.read_table(
            file,
            columns=[
                "pickup_at",
                "dropoff_at",
                "pickup_location_id",
                "dropoff_location_id",
            ],
        )
        locdf = locdf.to_pandas()
        # print(f"Size of pyarrow table: {locdf.shape}")
    except Exception:
        print("Wrong!!!!")
        return [],[]

    pickup_location_ids = locdf["pickup_location_id"].unique()

    return locdf, pickup_location_ids


@ray.remote(num_returns=2)
def fetch_to_object_store(locdf, pickup_location_id):
    return (
        pickup_location_id,
        locdf[locdf["pickup_location_id"].isin([pickup_location_id])],
    ), np.zeros(OBJECT_SIZE // 8)



def run_batch_training_with_object_store(
    files: List[str], models: List[BaseEstimator], verbose: bool=False) -> \
            Tuple[Tuple[str, str, List[Tuple[BaseEstimator, float]]], Dict[Any, Any]]:
    """
    An optimized verion of this function that uses scheduling
    strategy and fetches id-data from Ray's object store

    Args:
        List of parquet files to the NYC data
        List of model estimators to use to train

    Returns:
        the tuple of results from all the runs (tuples) and timings (dict)
    """

    print("Starting optimized run: each task fetching pre-loaded data from Ray object store...")
    start = time.time()

    # Store task references
    task_refs = []

    # Use a SPREAD scheduling strategy to load each
    # file on a separate node as an OOM safeguard.
    # This is not foolproof though! We can also specify a resource
    # requirement for memory, if we know what is the maximum
    # memory requirement for a single file.
    '''
    read_into_object_store_spread = read_into_object_store.options(
        scheduling_strategy="SPREAD"
    )

    # Dictionary of references to read tasks with file ids as keys and its
    # respective object reference in the object store as value
    # This remote task will stores respective object referrences to the batch-id
    # as part of the yield statement. 
    read_tasks_by_file = {
        files[file_id]: read_into_object_store_spread.remote(file)
        for file_id, file in enumerate(files)
    }

    # Iterate over all the object refs returned by the generator and
    # placed in the dictionary above
    for file, read_task_ref in read_tasks_by_file.items():
        # Note: We iterate over references and pass them to the tasks directly.
        # No actual data batch is passed, only the reference to it is to train and 
        # evaluate
        for pickup_location_id_and_data_batch_ref in iter(ray.get(read_task_ref)):
            task_refs.append(
                train_and_evaluate_optimized.remote(
                    pickup_location_id_and_data_batch_ref, file, models, verbose=verbose)
                )
    '''

    dfs = []
    ids = []
    for file_id, file in enumerate(files):
        d, i = get_locdf_and_ids.remote(file)
        dfs.append(d)
        ids.append(i)
    pickup_location_id_and_data_batch_refs = []
    data = []
    for i in range(len(dfs)):
        pickup_location_ids = ray.get(ids[i])
        ps = []
        ds = []
        for pickup_location_id in pickup_location_ids:
            p, d= fetch_to_object_store.remote(dfs[i], pickup_location_id) 
            ps.append(p)
            ds.append(d)
        pickup_location_id_and_data_batch_refs.append(ps)
        data.append(ds)

    train_test_set = []
    for i in range(len(dfs)):
        for j in range(len(data[i])):
            train_test_set.append(transform_and_split.remote(pickup_location_id_and_data_batch_refs[i][j], data[i][j]))
    for d in train_test_set:
        res = []
        for model in models:
            res.append(mmt.fit_and_score_sklearn_tuple.remote(d, model))
        task_refs.append(res)

    print(os.system('ray memory --stats-only'))

    # Block ray.get is delayed until we need to obtain results from each task
    results = []
    for t in task_refs:
        results.append(ray.get(t))
    #results = ray.get(task_refs)

    taken = time.time() - start
    count = len(results)
    # If result is None, then it means there weren't enough records to train
    results_not_none = [x for x in results if x is not None]
    count_not_none = len(results_not_none)

    # Sleep a moment for nicer output
    times_stats = {}
    times_stats["total_pickup_locations"] =  count
    times_stats["total_pickup_locations_trained"] =  count_not_none
    times_stats["total_models_trained"] =  count_not_none * len(models)
    times_stats["total_training_time"] =  round(taken, 3)

    print(os.system('ray memory --stats-only'))

    return results, times_stats
