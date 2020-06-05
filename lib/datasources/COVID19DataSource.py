from .datasource import DataSource
import pandas as pd
from regions import Regions


class COVID19DataSource(DataSource):
    
    DATA_FORMAT = None
    DATA_TYPE = None
    REGION_REPRESENTATION = None
    DATA_ITEMS = None
    DATA_ITEMS_INFO = None

    def __init__(self, data_items=None, regions=None, start_date=None, end_date=None):
        super().__init__(data_items, regions)
        self.start_date = start_date
        self.end_date = end_date

    # defined methods from parent class

    def _get_urls(self):
        urls = []
        template_url = 'https://raw.githubusercontent.com/datadista/datasets/master/COVID%2019/ccaa_covid19_{name}.csv'

        for data_item in self.data_items:
            url = template_url.format(name=data_item)
            urls.append(url)
        return urls
    
    def _manage_response(self, response):
        return response
      
    def _process_partial_data(self, partial_requested_data):
        
        region_representation_dict = Regions._get_property(self.regions, self.__class__.REGION_REPRESENTATION)
        representation_region_dict = dict(zip(region_representation_dict, self.regions))
        
        df = partial_requested_data.rename(columns={"cod_ine":"Region"})
        df.set_index(df.Region.astype(str).str.zfill(2),inplace=True, drop=True) # zfill used to change numbers 1, 2, 3... tu padded strings "01", "02"... (code ine)
        df = df.drop(['Region','CCAA'], axis='columns', errors='ignore')
        df = df.transpose()
        df = df[region_representation_dict]  # filter regions
        df.rename(columns=representation_region_dict,inplace=True)  # replace region representation by region itself
        df = df.astype('float64')
        df.columns = pd.MultiIndex.from_product([df.columns, [self.data_items[self.processed_urls]]])
        df.set_index(pd.to_datetime(df.index, format="%Y-%m-%d"),inplace=True)  
        df.columns.rename("Item",level=1,inplace=True)
        return df
