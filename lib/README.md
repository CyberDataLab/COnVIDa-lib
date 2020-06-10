# COnVIDa lib
Core python-based implementation of data sources collection

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
Highly granular resource which groups related pieces of information (_data items_, in-depth explained later) regarding a field of knowledge, linked with a third-party data repository to be consulted.
Current version of COnVIDa includes 5 data sources related to the COVID19 pandemic in Spain. These data sources are:

* **COVID19**: Information about the COVID19  pandemic published by [Datadista in GitHub](https://github.com/datadista/datasets/tree/master/COVID%2019) (who in turn retrieves such data from the [Ministerio de Sanidad](https://www.mscbs.gob.es/) and the [Instituto de Salud Carlos III](https://www.isciii.es/)).
    
* **INE**: Information about different aspects of the Spanish reality extracted from the [Spanish National Institute of Statistics](https://www.ine.es/).
    
* **Mobility**: Information about citizens' mobility. In this case the original data sources are actually both the [Google COVID-19 Community Mobility Reports](https://www.google.com/covid19/mobility/) and the [Apple COVID‑19 - Mobility Trends Reports](https://www.apple.com/covid19/mobility).

* **MoMo**: Information about the [mortality monitoring system](https://momo.isciii.es/public/momo/dashboard/momo_dashboard.html) handled by the Instituto de Salud Carlos III. 

* **AEMET**: Information about meteorological data stemming from the AEMET ([Agencia Estatal de Meteorología](https://opendata.aemet.es/)). 


### Data Item
Low-grain resource which codifies a specific piece of information and belongs to one of the aforementioned _Data Sources_. In the following, the implemented data items are listed by data source:

* **COVID19**: recovered, cases, PCR confirmed, test confirmed, deaths, hospitalized and ICU.
    
* **INE**: [physical activity](https://www.ine.es/jaxi/Tabla.htm?path=/t15/p419/a2017/p06/l0/&file=04013.px), [body mass index (BMI)](https://www.ine.es/jaxi/Tabla.htm?path=/t15/p420/a2014/p06/l0/&file=01004.px), [tobacco consumption](https://www.ine.es/jaxi/Tabla.htm?path=/t15/p419/a2011/p06/&file=06020.px), [household by family type](https://www.ine.es/jaxi/Tabla.htm?path=/t20/p274/serie/def/p02/l0/&file=02007.px), [households by occupation density](https://www.ine.es/jaxi/Tabla.htm?path=/t20/p274/serie/def/p05/l0/&file=03011.px) and [over 65 years old alone](https://www.ine.es/jaxi/Tabla.htm?path=/t20/p274/serie/def/p02/l0/&file=02014.px).
    
* **Mobility**: mobility in different spaces such as grocery and pharmacy, parks, residential, retail and recreation, transit stations, workplace and vehicles (driving).

* **MoMo**: daily observed deaths, lower and upper bounds of such series, the daily expected deaths, and the 1st and 99th percentiles of such series.

* **AEMET**: rainfall, maximum pressure, minimum pressure, maximum gust, isolation, maximum temperature, mean temperature, minimum temperature, wind speed, altitude and gust direction.


### Data Type
Perspective by which the data are interpreted and the respective analysis:

* **Temporal**: The data items are indexed by days, so they will show the daily values. In particular, _COVID19, Mobility, MoMo_ and _AEMET_ data items are temporal. _For instance, if we select the COVID19 cases in Murcia from 21/02/2020 until el 14/05/2020, the X axis will show all the days between those two dates, while Y axis will show the daily COVID19 cases in Murcia_. 
    
    
* **Geographical**: The data items are indexed by regions and the data is aggregated with absolute values. Concretely, _INE_ data items are geographical. It is worth mentioning that the user of this library could transform temporal data item to geographical perspective by applying any kind of aggregation scheme.  _For instance, in COnVIDa service, if we choose the analysis type by regions and select some temporal data items, then COnVIDa service will use the mean of those data items within the specified data ranges_. 


### Regions
Divisions of the territory that allow a more exhaustive and deeper collection and analysis. In particular, it is designed for the Spanish provinces. In this sense, _COnVIDa_ lib allows the filtering of the aforementioned data items by regions.


## User guidelines

The [test lib notebook](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/lib/test_lib.ipynb) contains usage examples of _COnVIDa lib_. The most important modules and functions are the following.

#### [`Regions class`](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/lib/regions.py)
Implements the required information for Regions management

##### `get_country_codes()` 
    Returns a list with the supported country codes. Up to now, only 'ES' for Spanish provinces is available. 


##### `get_regions(country_code='ES')` 
    Returns a list with the names of the Spanish provinces.

    Parameters
    - country_code: string of the descriptions. Up to now, only 'ES' for Spanish provinces is available.

***

#### [`COnVIDA class`](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/lib/convida.py)
Provides an interface for the library user to avoid the use of low-level functions.

##### `get_data_types()`
    Returns the implemented datatypes in string format.

##### `get_items_by_datasource(data_type)`
    Returns a dictionary with data sources as keys, and an array of associated data item names as values.
    
    Parameters
    - data_type: DataType of the data sources.

##### `get_descriptions_by_datasource(data_type, languange='ES')`

    Returns a dictionary with data sources as keys, and an array of associated data item descriptions as values.

    Parameters
    - data_type: DataType of the data sources
    - language: language of the descriptions. 
        'ES' for Spanish (default value),
        'EN' for English.

##### `get_units_by_datasource(data_type, languange='ES')`
    Returns a dictionary with data sources as keys, and an array of associated data item units as values.
    
    Parameters
    - data_type: DataType of the data sources
    - language: language of the units. 
        'ES' for Spanish (default value),
        'EN' for English.

#### `get_units_by_datasource(data_type, languange='ES')`
    Returns a dictionary with data sources as keys, and an array of associated data item units as values.
    
    Parameters
    - data_type: DataType of the data sources
    - language: language of the descriptions. 
        'ES' for Spanish (default value),
        'EN' for English.

#### `get_data_items(data_items='all', regions='ES', start_date=None, end_date=None, language='ES', errors='ignore')`
    Returns a DataFrame with the required information. 

    Parameters
    - data_items: list of data item names. By default, 'all' are collected.
    - regions: list of region names. By default, 'ES' refers to all Spanish provinces.
    - start_date: first day in pandas datetime to be considered in TEMPORAL data items. By default, None is established.
    - end_date: last day in pandas datetime to be considered in TEMPORAL data items. By default, None is established.
    - languange: language of the returned data. 
        'ES' for Spanish (default value),
        'EN' for English.
    - errors: action to be taken when errors occur.
        'ignore' tries to get all possible data items even if some can't be collected,
        'raise' throws an exception and the execution is aborted upon detection of any error. 


    If dates are passed, then it is assumed that TEMPORAL data items are required. Otherwise, a GEOGRAPHICAL retrieval is assumed.
    A TEMPORAL retrieval produces a DataFrame with daily [Date] as row indexer and [Region, Data Item] as column indexer.
    A GEOGRAPHICAL retrieval produces a DataFrame with [Region] as row indexer and [Data Item] as column indexer.

***

## Developer guidelines
