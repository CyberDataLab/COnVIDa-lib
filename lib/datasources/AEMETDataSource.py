from .datasource import DataSource
import pandas as pd
import datetime
import os
import json
from regions import Regions
import requests
import numpy as np


class AEMETDataSource(DataSource):
    """ 
    Class which constitutes a collection from the AEMET Data Source.

    Attributes
    ----------
    DATA_FORMAT : DataFormat
        Data Format of the resource (JSON or CSV)
    DATA_TYPE : DataType
        Data Type of the Data Source (TEMPORAL or GEOGRAPHICAL)
    REGION_REPRESENTATION : str
        Representation of the regions within the Data Source (iso_3166_2, ine code, ...)
    DATA_ITEMS : list of str
        Names of the data items literally used by the Data Source
    DATA_ITEMS_INFO : dic { str : dic { str : dic { str : str } } }
        Information of each Data Item of the Data Source. The internal name (DATA_ITEMS) are the keys, whereas the following aspects are the keys of the first nested dic.
            Display Name (used to change the third-party nomenclature to a desired custom one)
            Description (meaning of the Data Item)
            Data unit (metric of the Data Item values: kg, persons, etc.)
        The second nested dic correspond to the keys 'EN' and 'ES', containing the English and Spanish texts respectively.
    API_KEY : str
        A string containg an API KEY to use the AEMET service
    """

    DATA_FORMAT = None
    DATA_TYPE = None
    REGION_REPRESENTATION = None
    DATA_ITEMS = None
    DATA_ITEMS_INFO = None
    API_KEY = None

    def __init__(self, data_items=None, regions=None, start_date=None, end_date=None):
        """
        Creates a collection of the AEMET Data Source

        Parameters
        ----------
        data_items : list of str
            list of required data item names of the AEMET Data Source.
        regions : list of str
            list of required regions. 
        start_date : pd.datetime
            first day to be considered. 
        end_date : pd.datetime
            last day to be considered. 
        
        Returns
        -------
        Returns a configured AEMET Data Source object ready to be used for getting the data.

        Notes
        -----
        By default, parameters to None indicates an empty instance (used to initialize class attributes).
        """

        super().__init__(data_items, regions)

        # check dates (AEMET refresh with a four-day lag)
        limit = pd.to_datetime('today', format='%Y-%m-%d') - datetime.timedelta(days=4)

        if start_date is not None:
            if start_date >= limit:
                self.start_date = limit
            else:
                self.start_date = start_date

        if end_date is not None:
            if end_date >= limit:
                self.end_date = limit
            else:
                self.end_date = end_date

        # first time of use, API KEY will be None; read it from configuration file
        if self.__class__.API_KEY is None:
            readed_api_key = self.__read_api_key()
            if readed_api_key == "":
                print('WARNING: AEMET API KEY has not been established in data-sources-config file')
            else:
                self.__class__.API_KEY = readed_api_key

        self.query_parameters['api_key'] = self.__class__.API_KEY

        self.sleep_time_before_request = 2  # initial waiting time between requests, increased with SLEEP_TIME_INCREASE when 429 error occurs

    # defined methods from parent class

    def _get_urls(self):
        '''
        Builds the URLs of the resources to be visited (ideally containing the raw data).
        
        Returns
        -------
        list of str
            list of working URLs (usually containing only one url).
        '''
        urls = []
        aemet_stations_by_regions = self.__get_stations_by_regions()

        # requests per regions
        for aemet_stations in aemet_stations_by_regions.values():  # request per region
            if aemet_stations:
                url = (
                "https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{start}/fechafin/{end}/estacion/{station}".format(
                    start=self.start_date.strftime('%Y-%m-%dT%H:%M:%SUTC'),
                    end=self.end_date.strftime('%Y-%m-%dT%H:%M:%SUTC'),
                    station=aemet_stations))
                urls.append(url)
        return urls

    def _manage_response(self, response):
        '''
        This function is always executed after consulting each URL of this Data Source. 
        It should manage the HTTP response and return the JSON of the Data Source.

        Parameters
        ----------
        - response: HTTP Response
            it is a HTTP Response from requests.get() (Request library) of one URL.

        Returns
        -------
        json
            Return the JSON contained in the response
        '''

        rjson = response.json()

        if rjson['estado'] != 200:
            self.last_error = rjson['estado']
            return None

        r = requests.get(rjson['datos'], params=self.query_parameters, verify=False)

        if r.status_code != requests.codes.OK:
            self.last_error = r.status_code
            return None

        return r.json()

    def _process_partial_data(self, partial_requested_data):

        '''
        This function is always executed after managing the response of each URL of this Data Source. 
        The data should be processed from the external structure to the form of a DataFrame. 

        Parameters
        ----------
        partial_requested_data : json
            it is the requested JSON of one URL.

        Returns
        -------
        pd.DataFrame
            a DataFrame with [Region] as row indexer and [Data Item] as column indexer.
        '''

        # Convert to numeric
        for d in partial_requested_data:
            for param in self.DATA_ITEMS:
                try:
                    d[param] = float(d[param].replace(',', '.'))
                except:
                    d[param] = None

        df = pd.DataFrame(partial_requested_data)
        df.indicativo = df.indicativo.replace(self.__get_regions_by_stations())
        df.rename(columns={'indicativo': 'Region'}, inplace=True)
        df = df.pivot_table(values=self.data_items, index='fecha', columns='Region', aggfunc=np.mean).swaplevel(i=0,
                                                                                                                j=1,
                                                                                                                axis='columns')
        df.columns.rename("Item", level=1, inplace=True)
        df.set_index(pd.to_datetime(df.index, format="%Y-%m-%d"), inplace=True)
        return df

    # private methods

    def __read_api_key(self):
        """
        Reads the API KEY from the data sources configuration file

        Returns
        -------
        str
            the API KEY contained in the AEMET entry of the data souces configuration file
        """

        current_path = os.path.dirname(os.path.realpath(__file__))
        config_path = os.path.join(current_path, DataSource._CONFIG_PATH)
        config_file = os.path.join(config_path, 'data-sources-config.json')

        try:
            with open(config_file) as json_file:
                config = json.load(json_file)
        except FileNotFoundError as e:
            print("ERROR: data-sources-config file not found!")
            return None
        except json.JSONDecodeError as e:
            print("ERROR: data-sources-config file is not well built!", e)
            return None

        data_source_name = self.__class__.__name__
        api_key = config[data_source_name]['API_KEY']
        return api_key

    def __get_stations_by_regions(self):
        """
        Gets the aemet stations per region

        Returns
        -------
        dict {str : str}
            a dictionary with instance regions as keys, and a string with the aemet stations separated by commas as values.

        Notes
        -----
        * It is used for completing 'idema' argument in the AEMET API function (https://opendata.aemet.es/dist/index.html?#!/valores-climatologicos/Climatolog%C3%ADas_diarias)
        """
        aemet_stations = Regions._get_property(self.regions, self.__class__.REGION_REPRESENTATION)
        str_aemet_stations = {}

        pos_region = 0
        for stations in aemet_stations:
            str_aemet_stations[self.regions[pos_region]] = ''
            for station in stations:
                str_aemet_stations[self.regions[pos_region]] = str_aemet_stations[
                                                                   self.regions[pos_region]] + "," + station
            str_aemet_stations[self.regions[pos_region]] = str_aemet_stations[self.regions[pos_region]][1:]
            pos_region = pos_region + 1

        return str_aemet_stations

    def __get_regions_by_stations(self):
        """
        Gets the region per aemet station.

        Returns
        -------
        dict {str : str}
            a dictionary with aemet stations as keys, and instance regions as values.
        """
        aemet_stations = Regions._get_property(self.regions, self.__class__.REGION_REPRESENTATION)
        region_by_aemet_station = {}

        pos_region = 0
        for stations in aemet_stations:
            for station in stations:
                region_by_aemet_station[station] = self.regions[pos_region]
            pos_region = pos_region + 1

        return region_by_aemet_station
