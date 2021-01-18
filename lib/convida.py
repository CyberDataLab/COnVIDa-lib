import pandas as pd

pd.options.mode.chained_assignment = None
import numpy as np
import json
import re
from collections import defaultdict
from datatype import DataType
from regions import Regions

from datasources.INEDataSource import INEDataSource
from datasources.AEMETDataSource import AEMETDataSource
from datasources.COVID19DataSource import COVID19DataSource
from datasources.MobilityDataSource import MobilityDataSource
from datasources.MoMoDataSource import MoMoDataSource


class COnVIDa:
    """ 
    A dispatcher which manages user queries against different Data Sources.

    Attributes
    ----------
    __DATA_SOURCE_CLASSES : list of classes
        The list of implemented Data Sources classes
    __DATA_SOURCES_INITIALIZED : boolean
        Internal flag indicating whether the Data Sources have loaded the configuration. 
    """

    # POSSIBLE IMPROVEMENT: remove this class list and take it directly from the data-sources-config.json KEYs 
    # To do this, we would need to see how to reference each class from the string
    __DATA_SOURCE_CLASSES = [INEDataSource, AEMETDataSource, COVID19DataSource, MobilityDataSource,
                             MoMoDataSource]  # add when new classes are created

    __DATA_SOURCES_INITIALIZED = False

    @classmethod
    def get_data_types(cls):
        """
        Returns the implemented datatypes in string format.
        """

        data_types = []
        for data_type in DataType:
            data_types.append(str(data_type))
        return data_types

    @classmethod
    def get_data_items(cls, data_items='all', regions='ES', start_date=None, end_date=None, language='ES',
                       errors='ignore'):
        """
        Collects the required Data Items from associated Data Sources

        Parameters
        ----------
        data_items : list of str
            list of data item names. By default, 'all' are collected.
        regions : list of str
            list of region names. By default, 'ES' refers to all Spanish provinces.
        start_date : pd.datetime
            first day to be considered in TEMPORAL data items. By default, None is established.
        end_date : pd.datetime
            last day to be considered in TEMPORAL data items. By default, None is established.
        language : str
            language of the returned data. 
                'ES' for Spanish (default value),
                'EN' for English.
        errors : str
            action to be taken when errors occur.
                'ignore' tries to get all possible data items even if some can't be collected,
                'raise' throws an exception and the execution is aborted upon detection of any error. 

        Returns
        -------
        pd.DataFrame 
            a DataFrame with the required information.

        Notes
        -----
        If dates are passed, then it is assumed that TEMPORAL data items are required. Otherwise, a GEOGRAPHICAL retrieval is assumed.
        A TEMPORAL retrieval produces a DataFrame with daily [Date] as row indexer and [Region, Data Item] as column multiindexer.
        A GEOGRAPHICAL retrieval produces a DataFrame with [Region] as row indexer and [Data Item] as column indexer.
        """

        # if data sources are not initialized, lets read configurations
        if not cls.__DATA_SOURCES_INITIALIZED:
            cls.__init_data_sources()

        ##### check of parameters #####

        if data_items == 'all':
            data_items = cls.get_data_items_names(data_type=None, language=language)
        else:
            ## check if items are implemented ##

            # get all implemented items
            implemented_data_sources = cls.get_data_items_names(data_type=None, language=language)
            implemented_data_items = []
            for implemented_data_source in list(implemented_data_sources.keys()):
                implemented_data_items = implemented_data_items + implemented_data_sources[implemented_data_source]

            # check 
            successful_data_items = []
            for data_item in data_items:
                if data_item not in implemented_data_items:
                    print(f'WARNING: Item {data_item} is not implemented')
                else:
                    successful_data_items.append(data_item)

            if not successful_data_items:
                print('WARNING: No result found for the specified data items and conditions')
                return None

            data_items = successful_data_items

        if regions == 'ES':
            regions = Regions.get_regions('ES')

        if start_date is None or end_date is None:
            assumed_data_type = DataType.GEOGRAPHICAL
        else:
            assumed_data_type = DataType.TEMPORAL

        print("Assumed a " + str(assumed_data_type.name) + " data retrieval...")

        if assumed_data_type is DataType.TEMPORAL:
            if start_date > end_date:
                print('ERROR: start_date (' + str(start_date) + ') should be smaller or equal than end_date (' + str(
                    start_date) + ')')
                return None

            if end_date > pd.to_datetime('today').date():
                print('ERROR: end_date (' + str(end_date) + ') should not refer to the future')
                return None

        ### change data items (display names) to internal representation ###

        internalname_displayname_dict = cls._get_internal_names_mapping(assumed_data_type, data_items,
                                                                        language=language)  # get internal name - display name dict (then is used to rename again)
        if internalname_displayname_dict is None:
            return None
        data_items = list(internalname_displayname_dict.keys())  # change data_items to internal representation

        ### group data items by data source in dictionary ###

        # existing items for assumed data type
        items_by_source = cls.get_data_items_names(data_type=assumed_data_type,
                                                   language=language)  # dict with : source -> [item1, item2]
        items_by_assumed_data_type = []
        for items in items_by_source.values():
            items_by_assumed_data_type = items_by_assumed_data_type + items

        # group requested items by data sources
        requested_items_by_source = defaultdict(list)  # dict  datasource : [requested item 1, requested item 2, ...]
        for data_item in data_items:
            source_class_found = False
            source = 0
            while source < len(cls.__DATA_SOURCE_CLASSES) and not source_class_found:
                source_class_found = cls.__DATA_SOURCE_CLASSES[source].data_item_exists(data_item)
                source = source + 1
            if source_class_found:
                requested_items_by_source[cls.__DATA_SOURCE_CLASSES[source - 1]].append(data_item)
            else:
                # never should get there
                print('WARNING: Data source not found for item \'' + str(data_item) + '\'')

        ##### data retrieval #####
        df_all_data_sources = None

        ## get data by data source ##
        for DATA_SOURCE_CLASS in requested_items_by_source.keys():

            df_data_source = None

            data_items = requested_items_by_source[DATA_SOURCE_CLASS]

            # for temporal data type
            if assumed_data_type is DataType.TEMPORAL:

                df_data_source = DATA_SOURCE_CLASS(data_items, regions, start_date, end_date).get_data(errors)

                if df_data_source is not None:
                    df_data_source = cls.__complete_dates(df_data_source, start_date,
                                                          end_date)  # complete with nan values those days without info

            # for geographical data type
            elif assumed_data_type is DataType.GEOGRAPHICAL:
                df_data_source = DATA_SOURCE_CLASS(data_items, regions).get_data(errors)
            else:
                # never should get here
                return None

            # continuous joining of data from diverse data sources
            if df_data_source is not None:
                if df_all_data_sources is None:
                    df_all_data_sources = df_data_source.sort_index(axis=1)
                else:
                    df_all_data_sources = pd.concat([df_all_data_sources, df_data_source], axis='columns').sort_index(
                        axis=1)

        ## END: get data by data source ##

        if df_all_data_sources is None:
            print('WARNING: No result found for the specified data items and conditions')
            return None

        def rename_with_regex(col_name):
            for internal_name in list(internalname_displayname_dict.keys()):
                if re.match(f"^{internal_name}$|^{internal_name} \(", col_name):
                    return re.sub(pattern=internal_name, repl=internalname_displayname_dict[internal_name],
                                  string=col_name)
            return 'None'

        df_all_data_sources.rename(columns=rename_with_regex, level='Item', inplace=True)

        ### filter retrieved data to match the specific query determined by data_items, regions and dates ###

        # filter requested data_items (some data sources request in the same query more data items than the ones requested )
        df_all_data_sources = df_all_data_sources.loc[:, df_all_data_sources.columns.get_level_values('Item') != 'None']

        if assumed_data_type is DataType.TEMPORAL:
            df_all_data_sources = df_all_data_sources[(df_all_data_sources.index >= start_date) & (
                    df_all_data_sources.index <= end_date)]  # to filter requested dates (indexes)
            df_all_data_sources = df_all_data_sources.loc[:,
                                  df_all_data_sources.columns.get_level_values('Region').isin(
                                      regions)]  # to filter dates (indexes)
        else:
            df_all_data_sources = df_all_data_sources[
                df_all_data_sources.index.isin(regions)]  # to filter requested regions (indexes)

        df_all_data_sources = df_all_data_sources.loc[:, ~df_all_data_sources.columns.duplicated()]
        return df_all_data_sources

    @classmethod
    def get_data_items_names(cls, data_type: DataType = None, language='ES'):
        """
        Gets the implemented Data Item names by Data Source depending on the desired Data Type.

        Parameters
        ----------
        data_type: DataType
            Data type of the data sources (DataType.GEOGRAPHICAL or DataType.TEMPORAL).
        language: str
            language of the names. 
                'ES' for Spanish (default value),
                'EN' for English.

        Returns
        -------
            dict { str : arr of str }
                A dictionary with data sources as keys, and an array of associated data item names as values.
        """
        # check item type
        if data_type is not None and data_type not in list(DataType):
            print('ERROR: ' + str(data_type) + ' is not a valid DataType')
            return None

        return cls.__get_items_property_by_datasource(data_type=data_type, propert='display_name', language=language)

    @classmethod
    def get_data_items_descriptions(cls, data_type: DataType = None, language='ES'):
        """
        Gets the implemented Data Item descriptions by Data Source depending on the desired Data Type.

        Parameters
        ----------
        data_type: DataType
            Data type of the data sources (DataType.GEOGRAPHICAL or DataType.TEMPORAL). By default, None indicates that both GEOGRAPHICAL and TEMPORAL data types are queried.
        language: str
            language of the descriptions. 
                'ES' for Spanish (default value),
                'EN' for English.

        Returns
        -------
            dict { str : arr of str }
                A dictionary with data sources as keys, and an array of associated data item descriptions as values.
        """
        # check item type
        if data_type is not None and data_type not in list(DataType):
            print('ERROR: ' + str(data_type) + ' is not a valid DataType')
            return None

        return cls.__get_items_property_by_datasource(data_type=data_type, propert='description', language=language)

    @classmethod
    def get_data_items_units(cls, data_type: DataType = None, language='ES'):
        """
        Gets the implemented Data Item units by Data Source depending on the desired Data Type.

        Parameters
        ----------
        data_type: DataType
            Data type of the data sources (DataType.GEOGRAPHICAL or DataType.TEMPORAL). By default, None indicates that both GEOGRAPHICAL and TEMPORAL data types are queried.
        language: str
            language of the units. 
                'ES' for Spanish (default value),
                'EN' for English.

        Returns
        -------
            dict { str : arr of str }
                A dictionary with data sources as keys, and an array of associated data item units as values.
        """
        # check item type
        if data_type is not None and data_type not in list(DataType):
            print('ERROR: ' + str(data_type) + ' is not a valid DataType')
            return None

        return cls.__get_items_property_by_datasource(data_type=data_type, propert='data_unit', language=language)

    # semi private methods 

    @classmethod
    def _get_internal_names_mapping(cls, data_type: DataType, display_names, language):
        """
        Gets the internal names (used by the Data Sources) of the specified display names.

        Parameters
        ----------
        data_type: DataType
            Data type of the data source of the display names (DataType.GEOGRAPHICAL or DataType.TEMPORAL).
        display_names: list of str
            list of data item display names  
        language: str
            language of the display names. 
                'ES' for Spanish,
                'EN' for English.

        Returns
        -------
            dict { str : str }
                A dictionary with internal name as keys and display name as value.
        """

        # check item type
        if data_type is not None and data_type not in list(DataType):
            print('ERROR: ' + str(data_type) + ' is not a valid DataType')
            return None

        if not cls.__DATA_SOURCES_INITIALIZED:
            cls.__init_data_sources()

        # we create a dictionary internal_name -> display_name
        internalnames_displaynames_dic = {}

        # if internal representation, display_name -> display_name
        if language == 'internal':
            for display_name in display_names:
                internalnames_displaynames_dic[display_name] = display_name
        else:
            for display_name in display_names:
                for DATA_SOURCE_CLASS in cls.__DATA_SOURCE_CLASSES:
                    if DATA_SOURCE_CLASS.DATA_TYPE is data_type:
                        for data_item in DATA_SOURCE_CLASS.DATA_ITEMS:
                            if DATA_SOURCE_CLASS.DATA_ITEMS_INFO[data_item]['display_name'][language] == display_name:
                                internalnames_displaynames_dic[data_item] = display_name

            # Possible improvement: keep in memory the mapping display name -> dict name

            # print warnings for not implemented display names
            for display_name in display_names:
                if display_name not in list(internalnames_displaynames_dic.values()):
                    print(f"WARNING. Item {display_name} is not {data_type.name} data")

        return internalnames_displaynames_dic

        ## private methods

    @classmethod
    def __get_items_property_by_datasource(cls, data_type: DataType, propert, language):
        """
        Gets a property of the Data Sources of a specific Data Type.

        Parameters
        ----------
        data_type: DataType
            Data type of the data source of the display names (DataType.GEOGRAPHICAL or DataType.TEMPORAL).
        propert: str
            Property of the Data Items, namely 'display_name', 'description' or 'data_unit' 
        language: str
            language of the property. 
                'ES' for Spanish,
                'EN' for English,
                'internal' to get internal name of Data Items.

        Returns
        -------
            dict { str : list of str }
                A dictionary with Data Sources as keys and list of associated Data Item properties as value.
        """

        # if data sources are not initialized, lets read configurations
        if not cls.__DATA_SOURCES_INITIALIZED:
            cls.__init_data_sources()

        datasource_items_dic = {}

        if data_type is None:
            for DATA_SOURCE_CLASS in cls.__DATA_SOURCE_CLASSES:
                datasource_items_dic[DATA_SOURCE_CLASS.__name__] = []
                for data_item in DATA_SOURCE_CLASS.DATA_ITEMS:
                    if language == 'internal':
                        datasource_items_dic[DATA_SOURCE_CLASS.__name__].append(data_item)
                    else:
                        datasource_items_dic[DATA_SOURCE_CLASS.__name__].append(
                            DATA_SOURCE_CLASS.DATA_ITEMS_INFO[data_item][propert][language])
        else:
            for DATA_SOURCE_CLASS in cls.__DATA_SOURCE_CLASSES:
                if DATA_SOURCE_CLASS.DATA_TYPE is data_type:
                    datasource_items_dic[DATA_SOURCE_CLASS.__name__] = []
                    for data_item in DATA_SOURCE_CLASS.DATA_ITEMS:
                        if language == 'internal':
                            datasource_items_dic[DATA_SOURCE_CLASS.__name__].append(data_item)
                        else:
                            datasource_items_dic[DATA_SOURCE_CLASS.__name__].append(
                                DATA_SOURCE_CLASS.DATA_ITEMS_INFO[data_item][propert][language])

        return datasource_items_dic

    @classmethod
    def __complete_dates(cls, df, start_date, end_date):
        """
        Forces a DataFrame to contain a specific date range in the row indexer. Dates that do not exist in the original DataFrame are created and the associated column values are considered as NaN.

        Parameters
        ----------
        df: DataFrame
            a DataFrame with daily dates in row index
        start_date : pd.datetime
            first day to be considered in the df row index
        end_date : pd.datetime
            last day to be considered in the df row index

        Returns
        -------
            DataFrame
                The DataFrame df with the date range start_date - end_date in the row index, containing NaN values in the created ones.
        """
        dates = pd.date_range(start_date, end_date)
        return df.reindex(dates, fill_value=np.nan)

    @classmethod
    def __init_data_sources(cls):
        """
        Configures and prepares the Data Sources to be used. It is only necessary to be executed for the first time.
        """
        for DATA_SOURCE_CLASS in cls.__DATA_SOURCE_CLASSES:
            DATA_SOURCE_CLASS()._init_data_source()  # init data source
