import fsspec
import obstore
from obstore.fsspec import AsyncFsspecStore
from obstore.store import S3Store
from s3fs import S3FileSystem


print("============== obstore ===============")
store = S3Store.from_env(
    "my-s3-bucket",
    config={
        "aws_endpoint": "http://localhost:30002",
        "access_key_id": "minio",
        "secret_access_key": "miniostorage",
        "aws_allow_http": "true",                     # Allow HTTP connections
        "aws_virtual_hosted_style_request": "false",  # Use path-style addressing
    }
)


stream = obstore.list(store)

for list_result in obstore.list(store):
    if "test" in list_result[0]["path"]:
        print(list_result[0])


print("============== obstore + fsspec ===============")
# fsspec_store = AsyncFsspecStore(store)
fsspec.register_implementation("s3", AsyncFsspecStore)
fsspec_store = fsspec.filesystem("s3", store=store)

file_path = "s3://my-s3-bucket/test.json"
print(fsspec_store.ls("/"))
# Check if the file exists

# if fsspec_store.exists(file_path):
#     # Open the file and read its content
#     with fsspec_store.open(file_path, mode="r") as f:
#         content = f.read()
#         print("File content:")
#         print(content)
# else:
#     print(f"File {file_path} does not exist!")
#
