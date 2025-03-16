import fsspec
from obstore.fsspec import AsyncFsspecStore
from obstore.store import S3Store
from s3fs import S3FileSystem

# TODO: how to receive bucket name

class ObstoreFileSystem(AsyncFsspecStore):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
