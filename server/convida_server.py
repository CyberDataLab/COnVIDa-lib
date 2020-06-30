from convida import COnVIDa
from regions import Regions
from datatype import DataType
import pandas as pd
import re
import warnings
warnings.filterwarnings("ignore", 'This pattern has match groups')
import logging
import os
import h5py
import os.path


class convida_server():
    """
    COnVIDa service from a server perspective that dispatches user queries locally.
    A Data Cache is required to be generated, so it can be loaded in memory and used to address local queries.

    Attributes
    ----------
    __DATA_PATH : str
        the relative path to the folder containing the Data Cache file.
    __CACHE_PATH : str
        the relative path to the Data Cache file.
    __UPDATE_DAYS : int
        number of retroactive days to update  from the last contained in the Data Cache.
    __DATA : dic { DataType : pd.DataFrame }
        the Data Cache loaded in memory,
            DataType.TEMPORAL contains the DataFrame with temporal data items
            DataType.GEOGRAPHICAL contains the DataFrame with geographical data items
    __LOGGER : logging.logger
        internal system of log
    """

    # For correct operation with the frontend
    convida_path = os.getcwd()
    lib = os.path.join(convida_path, 'COnVIDa-lib')
    convida_server = os.path.join(lib, 'server')
    convida_server_data = os.path.join(convida_server, 'data\\')

    __DATA_PATH = convida_server_data
    __CACHE_PATH = None    # absolute path of the cache loaded in memory

    __UPDATE_DAYS = 5    # past days to query

    __DATA = {
        DataType.TEMPORAL : None,
        DataType.GEOGRAPHICAL : None
    }

    __LOGGER = None

    @classmethod
    def init_log(cls):
        """
        Initializes the log system which produces information in ./log/convida.log
        It must be executed just at the beginning, before any other function.
        """
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        cls.__LOGGER = logging.getLogger(cls.__name__)
        cls.__LOGGER.setLevel('INFO')
        file_handler = logging.FileHandler("log/convida.log")
        formatter = logging.Formatter(log_format)
        file_handler.setFormatter(formatter)
        cls.__LOGGER.addHandler(file_handler)


    @classmethod
    def load_data(cls, cache_filename=None):
        """
        Reads the Data Cache file and loads in __DATA class attribute both the TEMPORAL and GEOGRAPHICAL DataFrames.
        It must be executed at the beginning and every time the Data Cache file gets updated.

        Parameters
        ----------
        cache_filename : str
            Name of the generated HDF5 binary data file containing cached data. By default, None is assigned and the file is searched in the folder __DATA_PATH (class attribute)

        Notes
        -----
        * This COnVIDa-server example is designed to contain only ONE DATA CACHE FILE in the data dir.
        """
        if cache_filename is None:
            try:
                for file in os.listdir(cls.__DATA_PATH):
                    if re.match('cache_\d\d\d\d\-\d\d\-\d\d\.h5',file):
                        cache_filename = cls.__DATA_PATH+file
            except Exception as e:
                cls.__LOGGER.exception(f"ERROR finding cache file {cls.__CACHE_PATH}", str(e))
                raise

            if cache_filename is None:
                cls.__LOGGER.exception(f" Cache file (with format 'cache_\d\d\d\d\-\d\d\-\d\d\.h5') not found in {cls.__DATA_PATH}")
                raise Exception(f" Cache file (with format 'cache_\d\d\d\d\-\d\d\-\d\d\.h5') not found in {cls.__DATA_PATH}")


        elif cache_filename == cls.__CACHE_PATH:
            cls.__LOGGER.info(f"Load data avoided. {cache_filename} is just loaded in memory")
            return


        try:
            temporal_data = pd.read_hdf(path_or_buf=cache_filename,
                                                    key='temporal',
                                                    mode='r')
        except FileNotFoundError as e:
            cls.__LOGGER.exception(f"ERROR: Temporal data not found! Check if '{cache_filename}' exists")
            if cls.__DATA[DataType.TEMPORAL] is None:
                raise
        except Exception as e:
            cls.__LOGGER.exception(f"ERROR reading temporal data in '{cache_filename}'", str(e))
            raise


        try:
            geographical_data = pd.read_hdf(path_or_buf=cache_filename,
                                                        key='geographical',
                                                        mode='r')
        except FileNotFoundError as e:
            cls.__LOGGER.exception(f"ERROR: Geographical data not found! Check if '{cache_filename}' exists")
            if cls.__DATA[DataType.TEMPORAL] is None:
                raise
        except Exception as e:
            cls.__LOGGER.exception(f"ERROR reading geographical data in '{cache_filename}'", str(e))
            raise


        cls.__DATA[DataType.TEMPORAL] = temporal_data
        cls.__DATA[DataType.GEOGRAPHICAL] = geographical_data
        cls.__CACHE_PATH = cache_filename
        cls.__LOGGER.info("Data loaded in memory")
        return

    @classmethod
    def daily_update(cls) -> bool:
        """
        Updates the Data Cache (which is loaded in memory) FROM the last day cached minus the number of days indicated in class attribute __UPDATE_DAYS UNTIL today. This method removes the outdated file and creates the up-to-date file in the data path (class attribute__DATA_PATH) with the filename `cache_YYYY-MM-DD.h5` of today.

        Returns
        -------
        boolean
            True if the update was done, False otherwise.

        Notes
        ----
        * If this method notices that the cache filename corresponds to the date of today, it assumes that the Data Cache is up-to-date and nothing more is performed.
        * This function updates the Data Cache on disk, but load_data() function should be executed afterwards to perform the update in memory and, in turn, enable up-to-date queries.
        """
        # date of today
        today = pd.to_datetime(pd.to_datetime('today').strftime(format='%Y-%m-%d'))

        # check if daily update has been done before
        try:
            for file in os.listdir(cls.__DATA_PATH):
                if re.match(f"cache_{str(today)[0:10]}.h5", file):
                    cls.__LOGGER.info(f"Daily update avoided, the cache is up-to-date (today file cache_{str(today)[0:10]}.h5 already exists)")
                    return True
        except Exception as e:
            cls.__LOGGER.exception(f"ERROR finding cache file {new_cache_file}", str(e))
            return False


        # all regions
        all_regions = Regions.get_regions('ES')

        # new cache file
        new_cache_file = f"{cls.__DATA_PATH}cache_{str(today)[0:10]}.h5"


        # last cache file
        last_cache_file = cls.__CACHE_PATH


        ####### GEOGRAPHICAL UPDATE #######

        # all data items
        try:
            datasources = COnVIDa.get_items_by_datasource(DataType.GEOGRAPHICAL, language='internal')
            all_data_items = []
            for data_items in datasources.values():
                all_data_items += data_items


            new_geodata = COnVIDa.get_data_items(regions=all_regions,
                                                 data_items=all_data_items,
                                                 language='internal',
                                                 errors='raise')

        except Exception as e:
            cls.__LOGGER.exception("Retrieval of geographical data in daily update failed: ", str(e))
            return False


        ####### TEMPORAL UPDATE #######


        # all data items
        try:
            datasources = COnVIDa.get_items_by_datasource(DataType.TEMPORAL,language='internal')
            all_data_items = []
            for data_items in datasources.values():
                all_data_items += data_items


            last_date = cls.__DATA[DataType.TEMPORAL].index[-1]
            start_date = last_date-pd.DateOffset(days=cls.__UPDATE_DAYS)

            # get updated data of last days and today
            new_data = COnVIDa.get_data_items(regions=all_regions,
                                              data_items=all_data_items,
                                              start_date=start_date,
                                              end_date=today,
                                              language='internal',
                                              errors='raise')

            # update cache
            new_tempdata = cls.__DATA[DataType.TEMPORAL]
            new_tempdata = new_tempdata.append(new_data)
            new_tempdata = new_tempdata.loc[~new_tempdata.index.duplicated(keep='last')]


        except Exception as e:
            cls.__LOGGER.exception("Retrieval of temporal data in daily update failed: ", str(e))
            return False


        ####### COMPLETE UPDATE IF NEW DATA IS AVAILABLE ##########

        try:
            # create new files
            new_geodata.to_hdf(path_or_buf=new_cache_file,
                        key='geographical',
                        mode='a')
            new_tempdata.to_hdf(path_or_buf=new_cache_file,
                            key='temporal',
                            mode='a')
        except Exception as e:
            if os.path.exists(new_cache_file):
                os.remove(new_cache_file)  # remove created cache if daily update fail
            cls.__LOGGER.exception("Creation of new cache file in daily update failed:  ", str(e))
            return False


        # if the process has been stably completed, lets remove old cache
        # at this point, both old and new cache exist and new cache is in memory
        try:
            # at this point, both the old and new cache should exist: lets remove the old one
            if os.path.exists(new_cache_file) and os.path.exists(last_cache_file):
                os.remove(last_cache_file)  # remove created cache if daily update fail
                cls.__LOGGER.info("Daily update done!")
                return True
        except Exception as e:
            cls.__LOGGER.exception("Error in removing old cache file: ", str(e))

        ### should never get here ###
        try:
            # if error in removing old cache file, lets recover old status
            if os.path.exists(new_cache_file):
                os.remove(new_cache_file)
            cls.load_data(old_cache_file)
        except Exception as e:
            cls.__LOGGER.info("Critical fail in daily update: it was not possible to recover old status")
        return False


    @classmethod
    def get_data_items(cls, data_items: list, regions: list, start_date=None, end_date=None, language='ES'):
        """
        Locally gets the required information from a previously generated Data Cache

        Parameters
        -----------
        data_items: list of str
            Data item names.
        regions: list of str
            Region names.
        start_date: pd.datetime
            first day to be considered in TEMPORAL data items. By default, None is established.
        end_date: pd.datetime
            last day to be considered in TEMPORAL data items. By default, None is established.
        language:
            language of the returned data.
                'ES' for Spanish (default value),
                'EN' for English.

        Returns
        -------
        pd.DataFrame
            A TEMPORAL retrieval produces a DataFrame with daily [Date] as row indexer and [Region, Data Item] as column multiindexer.
            A GEOGRAPHICAL retrieval produces a DataFrame with [Region] as row indexer and [Data Item] as column indexer.

        Notes
        -----
        * The Data Cache should be loaded in memory.
        * If dates are passed, then it is assumed that TEMPORAL data items are required. Otherwise, a GEOGRAPHICAL retrieval is assumed.
        """
        try:
            if not isinstance(data_items, list):
                raise TypeError("Data items shoud be a list")

            if not isinstance(regions, list):
                raise TypeError("Regions shoud be a list")


            if start_date is None or end_date is None:
                assumed_data_type = DataType.GEOGRAPHICAL
            else:
                if start_date > end_date:
                    print('ERROR: start_date ('+str(start_date)+') should be smaller or equal than end_date ('+str(start_date)+')')
                    return None
                if end_date > pd.to_datetime('today').date():
                    print('ERROR: end_date ('+str(end_date)+') should not refer to the future')
                    return None

                assumed_data_type = DataType.TEMPORAL


            # change display names to internal representation
            internalname_displayname_dict = COnVIDa._get_internal_names_mapping(assumed_data_type, data_items, language=language)

            if internalname_displayname_dict is None:
                cls.__LOGGER.info("Mapping of display name to internal name failed! ", str(e))
                return None

            data_items = list(internalname_displayname_dict.keys())  # change data_items to internal representation

            # get data
            if assumed_data_type is DataType.GEOGRAPHICAL:
                data = cls.__get_geographical_items(data_items=data_items, regions=regions)
            else:
                data = cls.__get_temporal_items(data_items=data_items, regions=regions, start_date=start_date, end_date=end_date)

             # reverse operation of changing internal representation to display
            def rename_with_regex(col_name):
                for internal_name in list(internalname_displayname_dict.keys()):
                    if re.match(f"^{internal_name}$|^{internal_name} \(", col_name):
                        return re.sub(pattern=internal_name, repl=internalname_displayname_dict[internal_name], string=col_name)
                return col_name

            data.rename(columns=rename_with_regex, level='Item', inplace=True)
            return data

        except Exception as e:
            cls.__LOGGER.exception("Request get_data_items failed: ", str(e))
            return None


    @classmethod
    def get_min_date(cls):
        """
        Gets the first cached day (by default, 1st January 2016)

        Returns
        -------
        pd.datetime
            the date of the first cached day
        """
        return cls.__get_date(0)


    @classmethod
    def get_max_date(cls):
        """
        Gets the last cached day

        Returns
        -------
        pd.datetime
            the date of the last cached day
        """
        return cls.__get_date(-1)


    #### private methods ###

    @classmethod
    def __get_date(cls,index):
        """
        Gets the date of at a specific index position in the TEMPORAL data cache

        Returns
        -------
        pd.datetime
            the date contained in the row of the position "index"
        """
        loaded = True

        # this method should never be invoked before loading data...
        if cls.__DATA[DataType.TEMPORAL] is None:
            cls.init_log()
            loaded = cls.load_data()

        if loaded:
            return str(cls.__DATA[DataType.TEMPORAL].index[index])[0:10]
        else:
            cls.__LOGGER.info("min/max date request failed because no data loaded in memory", str(e))
            return None


    @classmethod
    def __get_temporal_items(cls, data_items, regions, start_date, end_date):
        """
        Resolves queries of temporal Data Items against the Data Cache

        Parameters
        ----------
        data_items: list of str
            Data item names.
        regions: list of str
            Region names.
        start_date: pd.datetime
            first day to be considered.
        end_date: pd.datetime
            last day to be considered.

        Returns
        -------
        pd.DataFrame
            a DataFrame from start_date to end_date in row index, regions at level 0 of multicolumn index and data_items at level 1 of multicolumn index.
        """
        temporal_data_df = cls.__DATA[DataType.TEMPORAL]

        # date filtering
        temporal_data_df = temporal_data_df[(temporal_data_df.index>=start_date) &
                                            (temporal_data_df.index<=end_date)]

        # data items & regions filtering
        regex_data_items = ['^'+data_item.replace('(','\(').replace(')','\)')+'$' for data_item in data_items]  # generate regex for requested data items to filter columns (specially necessary for MoMo)
        data_item_pattern = '|'.join(regex_data_items)
        temporal_data_df = temporal_data_df.iloc[:, temporal_data_df.columns.get_level_values('Region').isin(regions) &
                                                 temporal_data_df.columns.get_level_values('Item').str.match(pat=data_item_pattern)]

        return temporal_data_df

    @classmethod
    def __get_geographical_items(cls, data_items, regions):
        """
        Resolves queries of geographical Data Items against the Data Cache

        Parameters
        ----------
        data_items: list of str
            Data item names.
        regions: list of str
            Region names.

        Returns
        -------
        pd.DataFrame
            a DataFrame with regions at row index and data_items at column index.
        """

        geographical_data_df = cls.__DATA[DataType.GEOGRAPHICAL]

        # region filtering
        geographical_data_df = geographical_data_df[geographical_data_df.index.isin(regions)]

        # data items filtering
        pattern = '|'.join(data_items)
        geographical_data_df = geographical_data_df.filter(regex=pattern, axis='columns')

        return geographical_data_df