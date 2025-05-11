
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
This example will load a public Zarr array from S3 and compute the **maximum value within each chunk**, returning one row per chunk.
Make sure Spark is installed and configured, then run:

```bash
spark-submit example_spark_job.py
```

## 🧠 How It Works
- `tensorstore_zarr_rdd.py`: Defines a custom RDD that fetches Zarr chunks from S3 using TensorStore and computes the maximum value in each chunk.
- `tensorstore_zarr_loader.py`: Wraps the RDD and converts it into a Spark DataFrame.
- `example_spark_job.py`: Opens the Zarr spec and loads the per-chunk max values into Spark. Opens the Zarr spec and loads it into Spark.

## ✍️ Notes
- The array is chunked based on the native chunk size specified in the Zarr metadata.
- This chunking information is automatically extracted using TensorStore.
- The Zarr data must be stored in a format TensorStore understands (e.g., `.zarr` layout).

## 🛠️ Customization
You can extend this project to perform more meaningful or domain-specific processing on Zarr chunks. For example:

- Replace the `np.max(arr)` operation with:
  - `np.mean(arr)` to compute the average value per chunk
  - `np.std(arr)` for standard deviation
  - `np.count_nonzero(arr > threshold)` for threshold-based counts
  - `np.histogram(arr)` to compute per-chunk histograms 
  - extract keypoints and features from each chunk

- Add chunk index metadata to each row to help track spatial location:
  - Include `idx_start` in the returned tuple: `return [(idx_start, max_val)]`
  - Update the Spark schema accordingly

- Apply preprocessing:
  - Normalize or threshold chunk data
  - Mask out irrelevant data regions
  
- Chain this with downstream Spark processing:
  - Aggregate chunk stats across the full array
  - Join chunk-level data with metadata tables
  - Save summaries to Parquet or Delta Lake for reuse
- Add schema inference from Zarr metadata.
- Handle 3D or 4D data arrays.
- Write back to Parquet or Delta Lake.
