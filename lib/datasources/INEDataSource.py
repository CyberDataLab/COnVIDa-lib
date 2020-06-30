from .datasource import DataSource
import pandas as pd
from regions import Regions
import numpy as np

class INEDataSource(DataSource):
    """ 
    Class which constitutes a collection from the INE Data Source.

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
    """
    DATA_FORMAT = None
    DATA_TYPE = None
    REGION_REPRESENTATION = None
    DATA_ITEMS = None
    DATA_ITEMS_INFO = None
    
        
    def __init__(self, data_items=None, regions=None):
        """
        Creates a collection of the INE Data Source

        Parameters
        ----------
        data_items : list of str
            list of required data item names of the INE Data Source. By default, None indicates an empty instance (used to initialize class attributes).
        regions : list of str
            list of required regions. By default, None indicates an empty instance (used to initialize class attributes).
        
        Returns a configured INE Data Source object ready to be used for getting the data.
        """
        super().__init__(data_items,regions)
        
        
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
        template_url = 'http://servicios.ine.es/wstempus/js/ES/{funcion}/{codigo}?nult={num_datos}'

        for data_item in self.data_items:
            cod = self.__class__.DATA_ITEMS_INFO[data_item]['codigo'] + self.__class__.DATA_ITEMS_INFO[data_item]['_id']
            url = template_url.format(funcion=self.__class__.DATA_ITEMS_INFO[data_item]['funcion'], codigo=cod, num_datos=self.__class__.DATA_ITEMS_INFO[data_item]['num_datos'])
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
            the JSON contained in the response
        ''' 
        return response.json()

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
