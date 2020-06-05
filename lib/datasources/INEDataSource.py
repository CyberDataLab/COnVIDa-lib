from .datasource import DataSource
import pandas as pd
from regions import Regions
import numpy as np

class INEDataSource(DataSource):
    
    DATA_FORMAT = None
    DATA_TYPE = None
    REGION_REPRESENTATION = None
    DATA_ITEMS = None
    DATA_ITEMS_INFO = None
    
        
    def __init__(self, data_items=None, regions=None):
        super().__init__(data_items,regions)
        
        
    # defined methods from parent class
   
    def _get_urls(self):
        urls = []
        template_url = 'http://servicios.ine.es/wstempus/js/ES/{funcion}/{codigo}?nult={num_datos}'

        for data_item in self.data_items:
            cod = self.__class__.DATA_ITEMS_INFO[data_item]['codigo'] + self.__class__.DATA_ITEMS_INFO[data_item]['_id']
            url = template_url.format(funcion=self.__class__.DATA_ITEMS_INFO[data_item]['funcion'], codigo=cod, num_datos=self.__class__.DATA_ITEMS_INFO[data_item]['num_datos'])
            urls.append(url)
        return urls
    
    def _manage_response(self, response):
        return response.json()

    def _process_partial_data(self, partial_requested_data):
        
        if self.__class__.DATA_ITEMS_INFO[self.data_items[self.processed_urls]]['funcion'] == 'DATOS_TABLA':      

            df = pd.json_normalize(partial_requested_data,'Data',['Nombre'])
            df.Nombre = df.Nombre.replace(dict(zip(Regions._get_property(self.regions, self.__class__.REGION_REPRESENTATION),self.regions)), regex=True)  # some regions contains commas, fix it through configuration file

            # json field 'Nombre' is splitted into Region, SubItem and Item columns
            # SubItem is a subdivision of the seeked Item
            
            df[['Region','SubItem','Item']] = df['Nombre'].str.split(", ", n=2, expand=True)  
            
            # if in Region we find the word 'sexo', we should interchange Region and Subitem Columns
            if any(df.Region.str.contains(pat='sexo',case=False,regex=True)):
                df[['Region','SubItem']] = df[['SubItem','Region']]
            
            # Subitem is a subdivision of the seeked Item, but we want absolute values (Total, both sexs, etc.)
            c=None
            for category in df.SubItem.unique():
                if "total " in category.lower() or "ambos " in category.lower():
                    c = category
                    
            if c is None:
                print(f"WARNING! Revise the parsing of {self.data_items[self.processed_urls]} data item")
                return None
            
            
            df = df[df.SubItem==c] # filter to get only 'Both sexs' or 'Total' in existing SubItems
            df = df[~df.Item.str.contains(pat='Total',case=False,regex=True)] # remove Total columns
            df = df[df.Region.isin(self.regions)]  # filter regions
            df = df.pivot_table(index='Region',columns='Item', values='Valor', aggfunc=np.mean)
            df.columns = self.data_items[self.processed_urls] + " (" + df.columns + ")"
            return df
