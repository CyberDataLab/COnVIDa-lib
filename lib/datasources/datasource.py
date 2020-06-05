import pandas as pd
import requests
import numpy as np
from pandas.io.json import json_normalize  
import time
from urllib.request import urlopen
import json

import os

from enum import Enum

from datatype import DataType
from regions import Regions


# para no imprimir warnings
import urllib3
urllib3.disable_warnings()


class DataFormat(Enum):
    JSON = 0
    CSV = 1
    

class DataSource(object):
    
    __SLEEP_TIME_INCREASE = 0.5   # increment of sleep time to handle 429 http errors
    __SLEEP_TIME_LIMIT = 4         # limit of sleep time to abort requesting the data source
    _CONFIG_PATH = 'config'

        
    def __init__(self, data_items, regions):
        self.data_items = data_items
        self.regions = regions
        self.query_parameters = {}
        self.request_errors = 0
        self.last_error = 200
        self.processed_urls = 0  # attribute needed to manage the url (data items) being processed
        
        self.sleep_time_before_request = 0  # initial waiting time between requests, increased with SLEEP_TIME_INCREASE when 429 error occurs
        
    def get_data(self, errors):
        
        urls = self._get_urls()
        if urls is None:
            return None
        
        df = self._make_requests(urls, errors)
        if df is None:
            return None
                
        return df
   

    @classmethod
    def data_item_exists(cls, data_item):
        return data_item in cls.DATA_ITEMS

    
    # protected functions
    def _make_requests(self, urls, errors):
        
        requested_data = None
        processed_data = None
        
    
        for url in urls:
            partial_requested_data = self._make_request(url)

            # if request fails, WARNING and return
            if partial_requested_data is None:
                
                if errors=='ignore':
                    print(f"WARNING. Request failed to {str(self.__class__.__name__)} with HTTP {self.last_error} code.")
                    return None
                else:
                    raise Exception(f"Request failed to {str(self.__class__.__name__)} with HTTP {self.last_error} code")
            
            # if request works, unify partial requested data with previous answers
            else:
                partial_requested_data = self._process_partial_data(partial_requested_data)   # defined by each child data source (parse resource to template)
                
                if partial_requested_data is None:   
                    
                    if errors=='ignore':
                        print(f"WARNING. Processing of {str(self.__class__.__name__)} partial data failed.")
                        return None
                    else:
                        raise Exception(f"Processing of {str(self.__class__.__name__)} partial data failed.")
 
                # if requested_data is None is because the first data request  (nothing to unify)
                if requested_data is None:
                    requested_data = partial_requested_data
                    
                # unify is needed
                else:
                    requested_data = pd.concat([requested_data, partial_requested_data], axis='columns').sort_index(axis=1)
                
                self.processed_urls = self.processed_urls + 1 
        
        
        # requests done, if data does not exist, WARNING
        if requested_data is None:
            
            if errors=='ignore':
                print(f"WARNING. Request failed to {str(self.__class__.__name__)} with HTTP {self.last_error} code.")
                return None
            else:
                raise Exception(f"Request failed to {str(self.__class__.__name__)} with HTTP {self.last_error} code.")

        return requested_data
    
    
    
    # function for single request
    def _make_request(self, url):
        
        requested_data = None
        request_again = True

        while request_again:

            time.sleep(self.sleep_time_before_request)        # by default, sleep time is 0. If 429 http code is received, sleep time will be increased

            # json request
            if self.__class__.DATA_FORMAT is DataFormat.JSON:
                requested_data = requests.get(url, params=self.query_parameters, verify=False)
                self.last_error = requested_data.status_code
                
            # csv request      
            elif self.__class__.DATA_FORMAT is DataFormat.CSV:
                try:
                    requested_data = pd.read_csv(url, low_memory=False)
                    self.last_error = 200
                except Exception as e:
                    self.last_error = e.code
                    requested_data = None    
            else:
                # Never should get here
                requested_data = None
       
            # if OK
            if self.last_error == 200:
                requested_data = self._manage_response(requested_data)
                if requested_data is not None:
                    request_again = False
                    
            # if 'Too many requests' http error       
            elif self.last_error == 429:
                
                requested_data = None

                self.sleep_time_before_request  += self.__class__.__SLEEP_TIME_INCREASE

                if self.sleep_time_before_request > self.__class__.__SLEEP_TIME_LIMIT:
                    request_again = False
                #else:
                #    print(f"[{str(self.__class__.__name__)}] failed with {self.last_error} code, sleep {self.sleep_time_before_request }s and try again")
                    
            # other error, finish
            else:
                requested_data = None
                request_again = False

    
        return requested_data
    
    
    def _init_data_source(self):
        # fulfill class attributes in child nodes
        if self.__class__.DATA_FORMAT is None:
            self.__class__.DATA_FORMAT,self.__class__.DATA_TYPE,self.__class__.REGION_REPRESENTATION,self.__class__.DATA_ITEMS, self.__class__.DATA_ITEMS_INFO  = self.__read_config()

            
    def __read_config(self):
        
        ## read general info of the data source
        current_path = os.path.dirname(os.path.realpath(__file__))
        config_path = os.path.join(current_path,self.__class__._CONFIG_PATH)
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
        data_format = config[data_source_name]['DATA_FORMAT']
        if data_format == "json":
            data_format = DataFormat.JSON
        else:
            data_format = DataFormat.CSV
        data_type = config[data_source_name]['DATA_TYPE']
        if data_type == "temporal":
            data_type = DataType.TEMPORAL
        else:
            data_type = DataType.GEOGRAPHICAL
        region_representation = config[data_source_name]['REGION_REPRESENTATION']
        
        
        ## read specific info of data items
        items_config_path = os.path.join(config_path,'data_sources')
        items_config_file = os.path.join(items_config_path,f'{self.__class__.__name__}-config.json')

        try:
            with open(items_config_file, encoding="utf8") as json_file:
                data_items_info = json.load(json_file)
        except FileNotFoundError as e:
            print(f"ERROR: {self.__class__.__name__}-config file not found!")
            return None
        except json.JSONDecodeError as e:
            print(f"ERROR: {self.__class__.__name__}-config file is not well built!", e)
            return None
        
        data_items = list(data_items_info.keys())
        
        return data_format, data_type, region_representation, data_items, data_items_info
