## 1. Overview ##

Tmpo-py is a Python 2.7/3.x client library for tmpo. It synchronizes tmpo blocks with the Flukso REST API, caching them locally in a SQLite DB after download. A Pandas Time Series object can be built from these tmpo blocks with proper head/tail truncating.

## 2. Commands ##

Create a tmpo session object, which sets up a connection to the $HOME/.tmpo/tmpo.sqlite3 database. If the latter does not exist, it is initialized with the proper tables.

    >>> import tmpo
    >>> s = tmpo.Session()

Set the optional debug flag to see what is happening under the hood.

    >>> s.debug = True

Adding a sensor id + token combination will cause all tmpo blocks to be donwloaded for this specific sensor when running the sync command. Feel free to experiment with the Flukso HQ electricity data by adding this specific sensor.

    >>> s.add("fed676021dacaaf6a12a8dda7685be34", "b371402dc767cc83e41bc294b63f9586")

Synchronize and download tmpo blocks with the Flukso server. Optionally, one or multiple sensor id args can be specified to limit the syncing to those sensors.

    >>> s.sync()

Convert the time series data contained in the tmpo blocks to a Pandas TimeSeries data structure.

    >>> s.series("fed676021dacaaf6a12a8dda7685be34")

Provide optional head/tail arguments in Unix time to limit the time series length.

    >>> s.series("fed676021dacaaf6a12a8dda7685be34", head=1411043328, tail=1411043583)
    1411043332    3054225
    1411043358    3054226
    1411043383    3054227
    1411043408    3054228
    1411043434    3054229
    1411043458    3054230
    1411043481    3054231
    1411043505    3054232
    1411043528    3054233
    1411043553    3054234
    1411043577    3054235
    dtype: float64

