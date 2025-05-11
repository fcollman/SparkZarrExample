import tensorstore as ts

spec = {
    "driver": "zarr",
    "kvstore": "s3://ome-zarr-neuroglancer/1250417704.zarr/5/",
    "context": {"aws_credentials": {"profile": "anonymous"}},
}
print("hi")
z = ts.open(spec).result()
print("bye")
print(dir(z))
print(z.chunk_layout.read_chunk)
print(z)
