from .datasource import DataSource
import pandas as pd
from regions import Regions


class COVID19DataSource(DataSource):
    """ 
    Class which constitutes a collection from the COVID19 Data Source.

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

    def __init__(self, data_items=None, regions=None, start_date=None, end_date=None):
        """
        Creates a collection of the COVID19 Data Source

        Parameters
        ----------
        data_items : list of str
            list of required data item names of the INE Data Source.
        regions : list of str
            list of required regions. 
        start_date : pd.datetime
            first day to be considered. 
        end_date : pd.datetime
            last day to be considered. 
        
        Returns
        -------
        Returns a configured INE Data Source object ready to be used for getting the data.

        Notes
        -----
        By default, parameters to None indicates an empty instance (used to initialize class attributes).
        """
        super().__init__(data_items, regions)
        self.start_date = start_date
        self.end_date = end_date

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
        template_url = 'https://raw.githubusercontent.com/datadista/datasets/master/COVID%2019/ccaa_covid19_{name}.csv'

        for data_item in self.data_items:
            url = template_url.format(name=data_item)
            urls.append(url)
        return urls
    
    def _manage_response(self, response):
        '''
        This function is always executed after consulting each URL of this Data Source and gets the CSV associated. 

        Parameters
        ----------
        response : csv
            CSV from pandas.read_csv() (Pandas) of one URL.

        Returns
        -------
        csv
            the CSV contained in the response
        ''' 
        return response
      
    def _process_partial_data(self, partial_requested_data):
        '''
        This function is always executed after managing the response of each URL of this Data Source. 
        The data should be processed from the external structure to the form of a DataFrame. 

        Parameters
        ----------
        partial_requested_data : csv
            it is the requested CSV of one URL.

        Returns
        -------
        pd.DataFrame
            a DataFrame with daily [Date] as row indexer and [Region, Data Item] as column multiindexer.
        '''
        
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
