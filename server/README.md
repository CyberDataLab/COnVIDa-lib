# COnVIDa server
Example of COnVIDa service functionality

## Table of Contents

* [Principal elements and terminology](#principal-elements-and-terminology)
    * [Data Cache](#Data-Cache)
    * [Data Update](#Data-Update)
* [User guidelines](#user-guidelines)

## Principal elements and terminology
 
[_COnVIDa server_](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/server/convida_server.py) is an example of how to use _COnVIDa_ as a server side application. This means that the module is in charge of dispatching users' queries locally without consulting external resources, avoiding the inefficiency for [_COnVIDa lib_](https://github.com/CyberDataLab/COnVIDa-lib/tree/master/lib) to consult these original sources for every user request.

### Data Cache
The Data Cache is a persistent structure which contains the information of all the Data Sources and the corresponding Data Items within the desired date range. In particular, is an [HDF5](https://en.wikipedia.org/wiki/Hierarchical_Data_Format) binary data file (_.h5_) containing two main DataFrames for both `TEMPORAL`  and `GEOGRAPHICAL` data.

When implementing the [_COnVIDa server_](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/server/convida_server.py) for the first time, a Data Cache should be generated using the [data generation notebook](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/server/data_generation.ipynb). Note that the aforementioned notebook simply uses the [_COnVIDa lib_](https://github.com/CyberDataLab/COnVIDa-lib/tree/master/lib) to build the binary file with the desired Data Items, Regions and Dates. By default, all Data Items of all Data Sources are stored, and the date range contemplated for `TEMPORAL` data is from 1st January 2016 until today. Note that, with big date ranges, _AEMET Data Source_ may experience the 'too many requests' problem, correctable with the creation of the cache by batches).

The name of the Data Cache should follow the format `cache_YYYY-MM-DD.h5` (as in the [example](https://github.com/CyberDataLab/COnVIDa-lib/tree/master/server/data)), which specifically indicates the last day contemplated in the cache (that is, the last update). In addition, it should be placed within the `data/` folder.

### Data Update
Once the Data Cache has been created, we will probably want to update it with some frequency. To this end, [_COnVIDa server_](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/server/convida_server.py) implements the `daily_update()` function to append new data in the cache, respecting the update frequency of each data source. 

The update procedure of COnVIDa ensures that data is always up to date. With that objective, the thread daily checks the update frequency and the timestamp of the last update of each data source and, if required, it accordingly collects the necessary data series. It is worth noting that the temporal granularity of the time series should not necessarily coincide with the refresh time in the availability of the data in original repositories Additionally, it is also possible to indicate how many days backward to go from the last contemplated day. For example, AEMET updates its data with some days of delay. If the data cache is updated until yesterday, and the `daily_update()` is executed without 'days back', then we will only download empty values. That is why  [_COnVIDa server class_](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/server/convida_server.py) defines a default `DAILY UPDATE` of 20 days that are subtracted from the last day of the cache at the time of the update.


## User guidelines

The [test server lib notebook](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/server/test_server_lib.ipynb) contains usage examples of _COnVIDa server_. 

### [`convida_server class`](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/server/convida_server.py)
Implements the _COnVIDa service_ from a server perspective that dispatches user queries locally. A [Data Cache](#Data-Cache) must be generated, so it can be loaded in memory and used to address local queries.

##### `init_log()` 
    Initializes the log system which produces information in ./log/convida.log
    It must be executed just at the beginning, before any other function.

##### `load_data(cache_filename=None)` 
    Reads the Data Cache file and loads in __DATA class attribute both the TEMPORAL and GEOGRAPHICAL DataFrames. 
    It must be executed at the beginning and every time the Data Cache file gets updated.

    Parameters
    - cache_filename: Name of the generated HDF5 binary data file containing cached data. By default, None is assigned and the file is searched in the folder __DATA_PATH (class attribute)

    Notes
    * This COnVIDa-server example is designed to contain only ONE DATA CACHE FILE in the data dir.

##### `daily_update() -> bool` 
    Checks which data source should be refreshed and accordingly updates the Data Cache (which is loaded in memory) FROM the last day cached minus the number of days indicated in class attribute __UPDATE_DAYS UNTIL today. This method removes the outdated file and creates the up-to-date file in the data path (class attribute__DATA_PATH) with the filename `cache_YYYY-MM-DD.h5` of today.

    Returns True if the update was done, False otherwise.

    Notes
    * If this function notices that the cache filename corresponds to the date of today, it assumes that the Data Cache is up-to-date and it finishes.
    * This function updates the Data Cache on disk, but load_data() function should be executed afterwards to perform the update in memory and, in turn, enable up-to-date queries. 

##### `get_min_date()`
    Returns the first cached day (by default, 1st January 2016)

##### `get_max_date()`
    Returns the last cached day

##### `get_last_update_dates()`
    Gets the last update for each Data Source
    In particular, a pd.DataFrame with the date of the last update day (column), per Data Source (index)

##### `get_data_items(data_items: list, regions: list, start_date=None, end_date=None, language='ES')`
    Returns a DataFrame with the required information. 

    Parameters
    - data_items: list of data item names.
    - regions: list of region names.
    - start_date: first day in pandas datetime to be considered in TEMPORAL data items. By default, None is established.
    - end_date: last day in pandas datetime to be considered in TEMPORAL data items. By default, None is established.
    - language: language of the returned data. 
        'ES' for Spanish (default value),
        'EN' for English.

    Notes
    * The Data Cache should be loaded in memory.
    * If dates are passed, then it is assumed that TEMPORAL data items are required. Otherwise, a GEOGRAPHICAL retrieval is assumed.
    * A TEMPORAL retrieval produces a DataFrame with daily [Date] as row indexer and [Region, Data Item] as column multiindexer.
    * A GEOGRAPHICAL retrieval produces a DataFrame with [Region] as row indexer and [Data Item] as column indexer.
