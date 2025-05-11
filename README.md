# TensorStore Spark Loader

This project demonstrates how to load Zarr-format arrays stored in Amazon S3 directly into Apache Spark using [TensorStore](https://github.com/google/tensorstore) for efficient access.

## 🔧 Requirements
- Python 3.9+
- Apache Spark 3.x
- [uv](https://github.com/astral-sh/uv) for dependency management

## 📦 Setup
```bash
uv venv
uv pip install -e .
```

## 🧪 Example Usage
Make sure Spark is installed and configured, then run:

```bash
spark-submit example_spark_job.py
```

## 🧠 How It Works
- `tensorstore_zarr_rdd.py`: Defines a custom RDD that fetches Zarr chunks from S3 using TensorStore.
- `tensorstore_zarr_loader.py`: Wraps the RDD and converts it into a Spark DataFrame.
- `example_spark_job.py`: Opens the Zarr spec and loads it into Spark.

## ✍️ Notes
- The array is assumed to be chunked along the first axis.
- Modify `chunk_size` based on memory and parallelism requirements.
- The Zarr data must be stored in a format TensorStore understands (e.g., `.zarr` layout).

## 🛠️ Customization
You can extend this project to:
- Add schema inference from Zarr metadata.
- Handle 3D or 4D data arrays.
- Write back to Parquet or Delta Lake.
