# COnVIDa lib
#### Core implementation of data sources collection

## Principal elements and terminology
 
* **Data Source**: Highly granular resource which groups related pieces of information (_data items_, in-depth explained later) regarding a field of knowledge, linked with a third-party data repository to be consulted.
Current version of COnVIDa includes 5 data sources related to the COVID19 pandemic in Spain. These data sources are:

    * **COVID19**: Information about the COVID19  pandemic published by Datadista in Github (who in turn retrieves such data from the Ministerio de Sanidad and the Instituto de Salud Carlos III).
    
    * **INE**: Information about different aspects of the Spanish reality extracted from the Spanish National Institute of Statistics.
    
    * **Mobility**: Information about citizens' mobility. In this case the original data sources are actually both the Google COVID-19 Community Mobility Reports and the Apple COVID‑19 - Mobility Trends Reports.

    * **MoMo**: Information about the mortality monitoring system handled by the Instituto de Salud Carlos III. 

    * **AEMET**: Information about meteorological data stemming from the AEMET (Agencia Estatal de Meteorología). 


* **Data Item**: Low-grain resource which codifies a specific piece of information and belongs to one of the aforementioned _Data Sources_. In the following, the implemented data items are listed by data source:

    * **COVID19**: recovered, cases, PCR confirmed, test confirmed, deaths, hospitalized and ICU.
    
    * **INE**: physical activity, body mass index (BMI), tobacco consumption, household by family type, households by occupation density and over 65 years old alone.
    
    * **Mobility**: mobility in different spaces such as grocery and pharmacy, parks, residential, retail and recreation, transit stations, workplace and vehicles (driving).

    * **MoMo**: daily observed deaths, lower and upper bounds of such series, the daily expected deaths, and the 1st and 99th percentiles of such series.

    * **AEMET**: rainfall, maximum pressure, minimum pressure, maximum gust, isolation, maximum temperature, mean temperature, minimum temperature, wind speed, altitude and gust direction.


* **Data Type**: Perspective by which the data are interpreted and the respective analysis:

    * **Temporal**: The data items are indexed by days, so they will show the daily values. In particular, _COVID19, Mobility, MoMo_ and _AEMET_ data items are temporal. _For instance, if we select the COVID19 cases in Murcia from 21/02/2020 until el 14/05/2020, the X axis will show all the days between those two dates, while Y axis will show the daily COVID19 cases in Murcia_. 
    
    
    * **Geographical**: The data items are indexed by regions and the data is aggregated with absolute values. Concretely, _INE_ data items are geographical. It is worth mentioning that the user of this library could transform temporal data item to geographical perspective by applying any kind of aggregation scheme.  _For instance, in COnVIDa service, if we choose the analysis type by regions and select some temporal data items, then COnVIDa service will use the mean of those data items within the specified data ranges_. 


* **Regions**: Divisions of the territory that allow a more exhaustive and deeper collection and analysis. In particular, it is designed for the Spanish provinces. In this sense, _COnVIDa_ lib allows the filtering of the aforementioned data items by regions.


## User guidelines

The [test lib notebook](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/lib/test_lib.ipynb) contains usage examples of _COnVIDa lib_. The most important modules and functions are the following.

#### `Regions class`
Implements the required information for Regions management

##### `get_regions(language='ES')` 
    Returns a list with the names of the Spanish provinces.

    language - language of the descriptions. 'ES' for Spanish (default value), 'EN' for English.

***

#### `COnVIDA class`
Provides an interface for the library user to avoid the use of low-level functions.

##### `get_data_types()`
    Returns the implemented datatypes in string format.

##### `get_items_by_datasource(datatype)`
    Returns a dictionary with data sources as keys, and an array of associated data items as values.
    
    datatype - DataType of the data sources.

##### `get_descriptions_by_datasource(datatype, languange='ES')`

    Returns a dictionary with data sources as keys, and an array of associated data item descriptions as values.

    datatype - DataType of the data sources.
    language - language of the descriptions. 'ES' for Spanish (default value), 'EN' for English.

##### `get_units_by_datasource(datatype, languange='ES')`
    Returns a dictionary with data sources as keys, and an array of associated data item units as values.
    
    datatype - DataType of the data sources
    language - language of the descriptions. 'ES' for Spanish (default value), 'EN' for English.
    
***

## Developer guidelines
