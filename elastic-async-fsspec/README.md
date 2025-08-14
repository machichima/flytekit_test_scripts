# Elastic Spawn + Async + fsspec

Scripts in this folder is trying to find out why we get `function not awaited` and file
not downloaded error.

> Ref: https://flyte-org.slack.com/archives/CP2HDHKE1/p1753270501631199

## TODO

- Trying to reproduce the error only with fsspec/data_persistence.py + asyn.py + elastic spawn
    - Use `get_data` in `data_persistence`

