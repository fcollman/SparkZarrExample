import tensorstore as ts
from pyspark import SparkContext
from pyspark.rdd import RDD
import numpy as np
import itertools
from functools import partial
import time


def read_chunk_partial(idx_start, chunk_shape, full_shape, store_spec):
    start_total = time.time()
    print(f"Reading chunk at indices: {idx_start}")

    slices = tuple(
        slice(start, min(start + size, dim))
        for start, size, dim in zip(idx_start, chunk_shape, full_shape)
    )
    print(f"Slice spec: {slices}")

    start_open = time.time()
    z = ts.open(store_spec, open=True).result()
    print(f"Tensorstore open took {time.time() - start_open:.3f}s")
    print("Opened tensorstore")

    start_read = time.time()
    arr = z[slices].read().result()
    print(f"Tensorstore read took {time.time() - start_read:.3f}s")
    print(f"Chunk shape read: {arr.shape}")

    start_post = time.time()
    ##arr = np.asarray(arr)
    max_val = np.max(arr).item()
    print(f"Post-processing took {time.time() - start_post:.3f}s")
    print(f"Computed max value: {max_val}")
    print(f"Total time to read chunk: {time.time() - start_total:.3f}s")
    return [([max_val],)]


class TensorstoreZarrRDD:
    def __init__(
        self,
        spark_context: SparkContext,
        store_spec: dict,
        shape: tuple,
        dtype,
        native_chunk_shape: tuple,
    ):
        self.sc = spark_context
        self.store_spec = store_spec
        self.shape = shape
        self.dtype = dtype
        self.native_chunk_shape = native_chunk_shape

    def to_rdd(self) -> RDD:
        chunk_grid = [
            list(range(0, dim, chunk))
            for dim, chunk in zip(self.shape, self.native_chunk_shape)
        ]
        chunk_slices = list(itertools.product(*chunk_grid))

        func = partial(
            read_chunk_partial,
            chunk_shape=self.native_chunk_shape,
            full_shape=self.shape,
            store_spec=self.store_spec,
        )

        return self.sc.parallelize(chunk_slices, len(chunk_slices)).flatMap(func)
