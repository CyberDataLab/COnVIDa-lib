# COnVIDa lib
Core Python-based implementation of the data sources collection

## Table of Contents

* [Principal elements and terminology](#principal-elements-and-terminology)
    * [Data Source](#Data-Source)
    * [Data Item](#Data-Item)
    * [Data Type](#Data-Type)
    * [Regions](#Regions)
* [User guidelines](#user-guidelines)
* [Developer guidelines](#developer-guidelines)

## Principal elements and terminology
 
### Data Source
A Data Source is a highly granular resource which contains related pieces of information (_data items_, in-depth explained later) regarding a field of knowledge, linked to a third-party data repository to be consulted.
Current version of COnVIDa includes 5 data sources related to the COVID19 pandemic in Spain. These data sources are:

* **COVID19**: Information about the COVID19  pandemic published by [Datadista in GitHub](https://github.com/datadista/datasets/tree/master/COVID%2019) (who in turn retrieves such data from the [Ministerio de Sanidad](https://www.mscbs.gob.es/) and the [Instituto de Salud Carlos III](https://www.isciii.es/)).
    
* **INE**: Information about different aspects of the Spanish reality extracted from the [Spanish National Institute of Statistics](https://www.ine.es/).
    
* **Mobility**: Information about citizens' mobility. In this case the original data sources are actually both the [Google COVID-19 Community Mobility Reports](https://www.google.com/covid19/mobility/) and the [Apple COVID‑19 - Mobility Trends Reports](https://www.apple.com/covid19/mobility).

* **MoMo**: Information about the [mortality monitoring system](https://momo.isciii.es/public/momo/dashboard/momo_dashboard.html) handled by the Instituto de Salud Carlos III. 

* **AEMET**: Information about meteorological data stemming from the AEMET ([Agencia Estatal de Meteorología](https://opendata.aemet.es/)). **IMPORTANT**: To use this Data Source, an [API KEY](https://opendata.aemet.es/centrodedescargas/altaUsuario) is required, which should be then placed in the [Data Sources configuration file](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/lib/datasources/config/data-sources-config.json).


### Data Item
A Data Item is a low-grain resource which codifies a specific piece of information and belongs to one of the aforementioned _Data Sources_. Next, the data items currently implemented are listed by data source:

* **COVID19**: recovered, cases, PCR confirmed, test confirmed, deaths, hospitalized and ICU.
    
* **INE**: [physical activity](https://www.ine.es/jaxi/Tabla.htm?path=/t15/p419/a2017/p06/l0/&file=04013.px), [body mass index (BMI)](https://www.ine.es/jaxi/Tabla.htm?path=/t15/p420/a2014/p06/l0/&file=01004.px), [tobacco consumption](https://www.ine.es/jaxi/Tabla.htm?path=/t15/p419/a2011/p06/&file=06020.px), [household by family type](https://www.ine.es/jaxi/Tabla.htm?path=/t20/p274/serie/def/p02/l0/&file=02007.px), [households by occupation density](https://www.ine.es/jaxi/Tabla.htm?path=/t20/p274/serie/def/p05/l0/&file=03011.px) and [over 65 years old alone](https://www.ine.es/jaxi/Tabla.htm?path=/t20/p274/serie/def/p02/l0/&file=02014.px).
    
* **Mobility**: mobility in different spaces such as grocery and pharmacy, parks, residential, retail and recreation, transit stations, workplace and vehicles (driving).

* **MoMo**: daily observed deaths, lower and upper bounds of such series, the daily expected deaths, and the 1st and 99th percentiles of such series.

* **AEMET**: rainfall, maximum pressure, minimum pressure, maximum gust, isolation, maximum temperature, mean temperature, minimum temperature, wind speed, altitude and gust direction.


### Data Type
COnVIDa library considers two types of Data Items used to interpret and analyze them, namely:

* **Temporal**: The data items are indexed by time units (up to date, only days supported), so they will show in that temporal frequency. In particular, _COVID19, Mobility, MoMo_ and _AEMET_ data items are temporal. For instance, if we select the COVID19 cases in Murcia from 21/02/2020 until 14/05/2020, the X axis will show all the periods between those two dates, while Y axis will show the COVID19 cases in Murcia. 
    
* **Geographical**: The data items are indexed by region units. In particular, current _INE_ data items are geographical. It is worth mentioning that the user of this library could transform temporal data items to a geographical perspective by applying any kind of aggregation scheme. For instance, in COnVIDa service, if we choose the analysis type by regions and select some temporal data items, then COnVIDa service will descriptive statistical functions of those data items within the specified data ranges. 

### Temporal Granularity
The current release of COnVIDa library considers the following temporal units:

* **DAILY**: For temporal data sources, the data items should be presented by days. For creating new data sources to be directly integrated in the platform, developers should guarantee that granularity in the time series.

_More granularities can be supported in the future_
    
    
### Regional Granularity
The current release of COnVIDa library supports the following regional units:

* **COMMUNITY**: The data items can be presented per Spanish communities. 

* **PROVINCE**: The data items can be presented per Spanish provinces. 

_More granularities can be supported in the future_


## User guidelines

The [test lib notebook](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/lib/test_lib.ipynb) contains usage examples of the _COnVIDa lib_. The most important modules and functions are the following.

#### [`Regions class`](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/lib/regions.py)
Implements the required information for Regions management

##### `get_regions(country_code='ES')` 
    Returns a list with the names of the regions associated with a country code.

    Parameters
    - country_code: str
        country code of the regions to retrieve.
    
##### `get_regions_by_type(cls, type='c', country_code='ES')`
    Returns a list with the names of the regions of a specific type associated with a country code.

    Parameters
    - type: str
          For the country selected, the regional granularity to get. For Spain: 'c' Community, 'p' Province.
    - country_code: str
          country of the regions


##### `get_regions_population(cls, country_code='ES'):`
    
    Returns the number of citizens per region in a specific country

    Parameters
    - country_code: str
        Country code of the regions. 

##### `get_country_codes()` 
    Returns a dictionary with the supported countries as keys, and their codes as values.




***

#### [`COnVIDA class`](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/lib/convida.py)
Provides an interface for the library user to avoid the use of low-level functions.

##### `get_data_types()`
    Returns the implemented DataTypes in string format.

##### `get_sources_info()`
    Prints and returns a dictionary with the metadata about the supported data sources


##### `get_data_items_names(data_type=None, language='ES')`
    Returns a dictionary with data sources as keys, and an array of associated data item names as values.
    
    Parameters
    - data_type: DataType of the data sources (DataType.GEOGRAPHICAL or DataType.TEMPORAL). By default, None indicates that both GEOGRAPHICAL and TEMPORAL data types are queried.
    - language: language of the names. 
        'ES' for Spanish (default value),
        'EN' for English.

##### `get_data_items_descriptions(data_type=None, language='ES')`
    Returns a dictionary with data sources as keys, and an array of associated data item descriptions as values.

    Parameters
    - data_type: DataType of the data sources (DataType.GEOGRAPHICAL or DataType.TEMPORAL). By default, None indicates that both GEOGRAPHICAL and TEMPORAL data types are queried.
    - language: language of the descriptions. 
        'ES' for Spanish (default value),
        'EN' for English.

##### `get_data_items_units(data_type=None, language='ES')`
    Returns a dictionary with data sources as keys, and an array of associated data item units as values.
    
    Parameters
    - data_type: DataType of the data sources (DataType.GEOGRAPHICAL or DataType.TEMPORAL). By default, None indicates that both GEOGRAPHICAL and TEMPORAL data types are queried.
    - language: language of the units. 
        'ES' for Spanish (default value),
        'EN' for English.

##### `get_data_items(data_items='all', regions='ES', start_date=None, end_date=None, language='ES', errors='ignore')`
    Returns a DataFrame with the required information. 

    Parameters
    - data_items: list of data item names. By default, 'all' are collected.
    - regions: list of region names. By default, 'ES' refers to all Spanish regions.
    - start_date: first day in pandas datetime to be considered in TEMPORAL data items. By default, None is established.
    - end_date: last day in pandas datetime to be considered in TEMPORAL data items. By default, None is established.
    - language: language of the returned data. 
        'ES' for Spanish (default value),
        'EN' for English.
    - errors: action to be taken when errors occur.
        'ignore' tries to get all possible data items even if some can't be collected,
        'raise' throws an exception and the execution is aborted upon detection of any error. 


    If dates are passed, then it is assumed that TEMPORAL data items are required. Otherwise, a GEOGRAPHICAL retrieval is assumed.
    A TEMPORAL retrieval produces a DataFrame with daily [Date] as row indexer and [Region, Data Item] as column multiindexer.
    A GEOGRAPHICAL retrieval produces a DataFrame with [Region] as row indexer and [Data Item] as column indexer.

***

## Developer guidelines
_COnVIDa-lib_ constitutes an object-oriented package ready to be extended. Considering the [principal elements and terminology](#principal-elements-and-terminology), implementing a new Data Source (and associated data items) should be simple.

1. First of all, some elements should be defined regarding your new Data Source:
    * Name of the Data Source
    * Data Type of the Data Source (`TEMPORAL` or `GEOGRAPHICAL`)
    * Temporal Granularity the Data Source (`DAILY`)
    * Regional Granularity the Data Source (`COMMUNITIES or/and PROVINCES`)
    * Representation of the regions within the Data Source (_iso\_3166\_2_, _ine code_, ...)
    * Data Format of the resource (`JSON` or `CSV`)
    * Update Frequency of the data series (in days)
    * Information of each Data Item of the Data Source
        * Name (literally used by the Data Source)
        * Display Name (used to change the third-party nomenclature to a desired custom one)
        * Description (meaning of the Data Item)
        * Data unit (metric of the Data Item values: _kg_, _persons_, etc.)

2. Configure the aforementioned principal elements of your new Data Source:
    
    * The name, data type, temporal and regional granularities, region representation, data format, and update frequency should be included in the [data sources configuration file](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/lib/datasources/config/data-sources-config.json). With this aim, append a new entry in the JSON object with the data source name as a key, and a dictionary with the corresponding information regarding  `DATA TYPE`, `TEMPORAL GRANULARITY`, `REGIONAL GRANULARITY`, `REGION REPRESENTATION`, `DATA FORMAT`, and `UPDATE FREQUENCY` as values. If needed, specific config elements of your Data Source can be also included here (_for example, [AEMET data source](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/lib/datasources/AEMETDataSource.py) defines its `API KEY` necessary for it to work_).
    
    * For each region, the representation used by your Data Source should be appended accordingly in the [regions configuration file](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/lib/config/ES-regions.json) (in case it does not exist yet). Note that the key of the new entries to be added for each region should match with the aforementioned `REGION REPRESENTATION` attribute (defined in [data sources configuration file](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/lib/datasources/config/data-sources-config.json)).

    * The information of the Data Items offered by your Data Source should be included in a new configuration file `YourDataSourceName-config.json` in the [specific data source configuration folder](https://github.com/CyberDataLab/COnVIDa-lib/tree/master/lib/datasources/config/data_sources). As in the other configuration files residing in that folder (which may guide you in this procedure), each Data Item should constitute an entry. In particular, each entry is defined by the Data Item name (literally used by the Data Source) as the key and the properties `display_name`, `description` and `data_unit` as the values. The latter should include, in turn, translation in both Spanish and English (or any other language you may define). If needed, specific properties of your Data Items can be also included here (for example, the [Mobility data source](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/lib/datasources/config/data_sources/MobilityDataSource-config.json) includes the `data_source` attribute to distinguish the resource where each Data Item comes from).


3. Create your Data Source class in a new empty Python file `YourDataSourceName.py` and place it within the `/datasources` folder together with the rest of available Data Sources. 

4. Develop your new Data Source class as follows. Notice that the [datasources folder](https://github.com/CyberDataLab/COnVIDa-lib/tree/master/lib/datasources) contains other implemented Data Sources which may help you.

    * Extend the parent [`datasource class`](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/lib/datasources/datasource.py) (which already implements functionalities in terms of configuration and network management for you, common to all Data Sources): 
        ```python
        from .datasource import DataSource
        import pandas as pd
        from regions import Regions

        class YourNewDataSourceName(DataSource):
        ````

    * Declare to `None` the following class attributes:
        ```python
        DATA_TYPE = None
        TEMPORAL_GRANULARITY = None
        REGIONAL_GRANULARITY = None
        REGION_REPRESENTATION = None
        DATA_FORMAT = None
        UPDATE_FREQUENCY = None
        DATA_ITEMS = None
        DATA_ITEMS_INFO = None
        ```
        In the first execution of the class, these class attributes will load the values from the config files.


    * Define and fulfill the following functions Specifically, the function which processes partial data should apply the necessary transformations to return data compliant with standard temporal and regional granularity:

        ```python
        def __init__(self, data_items=None, regions=None, start_date=None, end_date=None):
        '''
        Constructor of the class which returns an instantiation of this DataSource responsible for collecting a selection of data items, for a group of regions, within a specifc date range. 
        
        Parameters
        - data_items: list of data item names. By default, 'all' are collected.
        - regions: list of region names. By default, 'ES' refers to Spanish regions.
        - start_date: first day in pandas datetime to be considered in TEMPORAL data items. By default, None is established.  If the Data Source is a GOGRAPHICAL data type, then it can be supressed.
        - end_date: last day in pandas datetime to be considered in TEMPORAL data items. By default, None is established.  If the Data Source is a GOGRAPHICAL data type, then it can be supressed.
        '''
            super().__init(data_items, regions) # call parent constructor
            self.start_date = start_date
            self.end_date = end_date

            # initialize the rest of specific object attributes
            ...
        ```

        ```python
        def _get_urls(self):
        '''
        Builds the URLs of the resources to be visited (ideally containing the raw data).
        This method must return a list of working URLs (usually only one url is needed).
        '''
        ```

        ```python
        def _manage_response(self, response):
        '''
        This function is always executed after consulting each URL of this Data Source. It should manage the HTTP response and return the JSON or CSV accordingly to the DATA FORMAT of the Data Source. 
        
        Parameters
        - response: 
            If DATA FORMAT of this Data Source is JSON, then it is a HTTP Response from requests.get() (Request library) of one URL.
            If DATA FORMAT of this Data Source is CSV, then it is a CSV from pandas.read_csv() (Pandas) of one URL.
        
        Notes
        * Ideally, if the response contains directly the data itself, it is only necesary to 
            in the case of JSON DATA FORMAT, return response.json()  
            in the case of CSV DATA FORMAT, return response.
          On the contrary, here the additional network management should be implemented to return the JSON or CSV.
        
        * The inherited variable self.processed_urls is available to control the url being processed.
        ''' 
        ```

        ```python
        def _process_partial_data(self, partial_requested_data):
        '''
        This function is always executed after managing the response of each URL of this Data Source. The data should be processed from the external structure to the form of a DataFrame that:
            in the case of TEMPORAL data type: daily [Date] as row indexer and [Region, Data Item] as column multiindexer.
            in the case of GEOGRAPHICAL data type: [Region] as row indexer and [Data Item] as column indexer.

        Parameters
        - partial_requested_data:
            If DATA FORMAT of this Data Source is JSON, then it is the requested JSON of one URL.
            If DATA FORMAT of this Data Source is CSV, then it is the requested CSV of one URL.

        Notes
        * It is really important to be compliant with the indicated DataFrame structure in order to allow the COnVIDa library to join the data items of this Data Source together (in the parent class) if necessary, as well as to allow the combination with other Data Sources in the same DataFrame (in COnVIDa wrapper class).

        * The inherited variable self.processed_urls is available to control the url being processed.

        * In order to filter the regions in partial_requested_data, the Regions module implements a function to get a list of the region codification of the desired regions:
            Regions._get_property(self.regions, self.__class__.REGION_REPRESENTATION)
        '''
        ```

        
    
    * If you needed to configure extra elements for the Data Source, you will need to read those extra elements on your own. A good idea is to define them as class attributes and initialize them reading the configuration file. For example, the [constructor of the AEMET data source](https://github.com/CyberDataLab/COnVIDa-lib/blob/a53f200f735efb3643a57b95bdea643aaf5d3ed8/lib/datasources/AEMETDataSource.py#L39) uses the [`__read_api_key()` function](https://github.com/CyberDataLab/COnVIDa-lib/blob/a53f200f735efb3643a57b95bdea643aaf5d3ed8/lib/datasources/AEMETDataSource.py#L108) through a singleton pattern to load the `API_KEY`.

        If you needed to configure extra properties for the Data Items, you will have access to them through the `DATA_ITEMS_INFO` class attribute. Specifically, `DATA_ITEMS_INFO[data_item_name][extra_property_name]` contains the value of the `extra_property_name` extra property for the `data_item_name` Data Item. For example, the [Mobility data source](https://github.com/CyberDataLab/COnVIDa-lib/blob/a53f200f735efb3643a57b95bdea643aaf5d3ed8/lib/datasources/MobilityDataSource.py#L29) does so to read the `data_source` property of its Data Items.



4. Your Data Source is already implemented! It is ready to be used **individually** through the class constructor (_\_\_init\_\__) and the method `get_data()`.

5. [_OPTIONAL_] It is **recommended** to wrap your Data Source with the [COnVIDa class](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/lib/convida.py) and, in turn, enable the available [easy-to-use functions](#COnVIDA-class]) instead of directly interacting with your low-level class. In addition, you will be able to use your new Data Source together with the rest, as shown in the [example notebook](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/lib/test_lib.ipynb). In this sense, it is required to integrate your new Data Source in the [wrapper factory COnVIDa class](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/lib/convida.py) as follows:

    * Import your new Data Source class:
    ```python
    from datasources.YourNewDataSourceName import YourNewDataSourceName
    ```

    * Add your new Data Source class in the class attribute `__DATA_SOURCE_CLASSES`
     ```python
    __DATA_SOURCE_CLASSES = [INEDataSource, AEMETDataSource, COVID19DataSource, MobilityDataSource, MoMoDataSource, YourNewDataSourceName]        
    ```   

    * That's it! Your new Data Source is ready to retrieve data from external resources as shown in the [example notebook](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/lib/test_lib.ipynb) through the [easy-to-use COnVIDa functions](#COnVIDA-class]). In fact, you should use the [example notebook](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/lib/test_lib.ipynb) to test/debug your class.

