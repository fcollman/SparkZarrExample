from pyspark.sql import SparkSession
from tensorstore_spark_loader.tensorstore_zarr_loader import load_zarr_to_dataframe
import tensorstore as ts
import numpy as np

spark = (
    SparkSession.builder.appName("TensorStoreZarrLoader")
    .master("local[*]")
    .getOrCreate()
)

store_spec = {
    "driver": "zarr",
    "kvstore": {
        "driver": "s3",
        "bucket": "ome-zarr-neuroglancer",
        "path": "1250417704.zarr/1/",
    },
    "context": {"aws_credentials": {"profile": "anonymous"}},
}

z = ts.open(store_spec).result()
print("Tensorstore opened successfully")
shape = z.shape
dtype = z.dtype
chunk_shape = z.chunk_layout.read_chunk.shape
print(f"Shape: {shape}, Dtype: {dtype}, Chunk shape: {chunk_shape}")

df = load_zarr_to_dataframe(spark, store_spec, shape, dtype, chunk_shape)
print("Collecting data...")
rows = df.collect()
print(f"Retrieved {len(rows)} rows")
print(df)
