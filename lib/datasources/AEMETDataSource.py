from .datasource import DataSource
import pandas as pd
import datetime
import os
import json
from regions import Regions
import requests
import numpy as np

class AEMETDataSource(DataSource):
    
    DATA_FORMAT = None
    DATA_TYPE = None
    REGION_REPRESENTATION = None
    DATA_ITEMS = None
    DATA_ITEMS_INFO = None
    API_KEY = None

    
    def __init__(self, data_items=None, regions=None, start_date=None, end_date=None):
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
            if  readed_api_key == "":
                print('WARNING: AEMET API KEY has not been established in data-sources-config file')
            else:
                self.__class__.API_KEY = readed_api_key

        
        self.query_parameters['api_key'] = self.__class__.API_KEY
         
        self.sleep_time_before_request = 2 # initial waiting time between requests, increased with SLEEP_TIME_INCREASE when 429 error occurs


    # defined methods from parent class
    
    def _get_urls(self):
        
        urls = []
        aemet_stations_by_regions = self.__get_stations_by_regions()
        
        # requests per regions
        for aemet_stations in aemet_stations_by_regions.values():    # request per region
            url = ("https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{start}/fechafin/{end}/estacion/{station}".format(
               start=self.start_date.strftime('%Y-%m-%dT%H:%M:%SUTC'),
               end=self.end_date.strftime('%Y-%m-%dT%H:%M:%SUTC'),
               station=aemet_stations))
            urls.append(url)
        
        return urls
    
        
    def _manage_response(self, response):

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
        
        # Convert to numeric
        for d in partial_requested_data:
            for param in self.DATA_ITEMS:
                try:
                    d[param] = float(d[param].replace(',', '.'))
                except:
                    d[param] = None
                    
        df = pd.DataFrame(partial_requested_data)
        df.indicativo = df.indicativo.replace(self.__get_regions_by_stations())
        df.rename(columns={'indicativo':'Region'},inplace=True)
        df = df.pivot_table(values=self.data_items, index='fecha', columns='Region', aggfunc=np.mean).swaplevel(i=0,j=1,axis='columns')
        df.columns.rename("Item",level=1,inplace=True)
        df.set_index(pd.to_datetime(df.index, format="%Y-%m-%d"),inplace=True)  
        return df
    
    
    # private methods
        
    def __read_api_key(self):
        
        current_path = os.path.dirname(os.path.realpath(__file__))
        config_path = os.path.join(current_path, DataSource._CONFIG_PATH)
        config_file = os.path.join(config_path,'data-sources-config.json')
        
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
        
        aemet_stations = Regions._get_property(self.regions, self.__class__.REGION_REPRESENTATION)
        str_aemet_stations = {}
                
        pos_region = 0
        for stations in aemet_stations:
            str_aemet_stations[self.regions[pos_region]] = ''
            for station in stations:
                str_aemet_stations[self.regions[pos_region]] = str_aemet_stations[self.regions[pos_region]]+","+station
            str_aemet_stations[self.regions[pos_region]] = str_aemet_stations[self.regions[pos_region]][1:] 
            pos_region = pos_region+1
        
        return str_aemet_stations
    
    
    def __get_regions_by_stations(self):
        
        aemet_stations = Regions._get_property(self.regions, self.__class__.REGION_REPRESENTATION)
        region_by_aemet_station = {}
        
        pos_region = 0
        for stations in aemet_stations:
            for station in stations:
                region_by_aemet_station[station] = self.regions[pos_region]
            pos_region = pos_region+1
            
        return region_by_aemet_station