from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, ArrayType
from .tensorstore_zarr_rdd import TensorstoreZarrRDD


def load_zarr_to_dataframe(
    spark: SparkSession, store_spec: dict, shape: tuple, dtype, chunk_shape: tuple
):
    sc = spark.sparkContext

    import numpy as np
    from pyspark.sql.types import (
        DoubleType,
        FloatType,
        IntegerType,
        ShortType,
        ByteType,
        LongType,
    )

    dtype_map = {
        "float32": FloatType(),
        "float64": DoubleType(),
        "int64": LongType(),
        "int32": IntegerType(),
        "int16": ShortType(),
        "int8": ByteType(),
        "uint8": ShortType(),
    }

    dtype_name = dtype.name
    spark_type = dtype_map.get(dtype_name, FloatType())

    schema = StructType([StructField("chunk_data", ArrayType(spark_type))])

    rdd_wrapper = TensorstoreZarrRDD(sc, store_spec, shape, dtype, chunk_shape)
    rdd = rdd_wrapper.to_rdd()

    df = spark.createDataFrame(rdd, schema=schema)
    return df
