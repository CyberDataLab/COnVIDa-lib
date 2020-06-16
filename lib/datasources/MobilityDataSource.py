from .datasource import DataSource
import pandas as pd
from regions import Regions


class MobilityDataSource(DataSource):
    """ 
    Class which constitutes a collection from the Mobility Data Source.

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
            Data source (Google or Apple)
        The second nested dic correspond to the keys 'EN' and 'ES', containing the English and Spanish texts respectively.
    """"
    DATA_FORMAT = None
    DATA_TYPE = None
    REGION_REPRESENTATION = None
    DATA_ITEMS = None
    DATA_ITEMS_INFO = None

    
    def __init__(self, data_items=None, regions=None, start_date=None, end_date=None):
                """
        Creates a collection of the Mobility Data Source

        Parameters
        ----------
        data_items : list of str
            list of required data item names of the Mobility Data Source.
        regions : list of str
            list of required regions. 
        start_date : pd.datetime
            first day to be considered. 
        end_date : pd.datetime
            last day to be considered. 
        
        Returns
        -------
        Returns a configured Mobility Data Source object ready to be used for getting the data.

        Notes
        -----
        By default, parameters to None indicates an empty instance (used to initialize class attributes).
        """
        super().__init__(data_items,regions)
        self.start_date = start_date
        self.end_date = end_date
        
        # google_detected : boolean ; google data items are requested in block, so this control variable is for parsing only in the first google-based data item 
        self.google_detected = False   

        # google_detected : boolean ; google data items are requested in block, so this control variable is for parsing only in the first google-based data item 
        self.apple_detected = False    

    # defined methods from parent class


    def _get_urls(self):
        '''
        Builds the URLs of the resources to be visited (ideally containing the raw data).
        
        Returns
        -------
        list of str
            list of working URLs (usually containing only one url).
        '''
        urls=[]
        data_item = 0
        while (data_item < len(self.data_items)) and (not self.google_detected or not self.apple_detected):
            if self.__class__.DATA_ITEMS_INFO[self.data_items[data_item]]['data_source'] == 'Google':
                if not self.google_detected:
                    self.google_detected = True
            elif self.__class__.DATA_ITEMS_INFO[self.data_items[data_item]]['data_source'] == 'Apple':
                if not self.apple_detected:
                    self.apple_detected = True
            data_item += 1
        
        if self.google_detected:
            urls.append('https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv')
        
        if self.apple_detected:
            urls.append('https://raw.githubusercontent.com/ActiveConclusion/COVID19_mobility/master/apple_reports/applemobilitytrends.csv')
        
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
        
        region_english_names = Regions._get_property(self.regions, self.__class__.REGION_REPRESENTATION)         # english names
        english_spanish_dict = dict(zip(region_english_names,self.regions)) # dict: {region english name : region spanish name}
   
        # if google detected, will be the first url (appending order in _get_urls)
        if self.google_detected:
            self.google_detected = False
            df = partial_requested_data[partial_requested_data.sub_region_1.isin(region_english_names)]
            df.sub_region_1.replace(to_replace=english_spanish_dict,inplace=True)
            df.drop(['country_region_code','country_region','sub_region_2'], axis='columns', errors='ignore', inplace=True)
            df = df.pivot_table(index='date',columns='sub_region_1').swaplevel(i=0,j=1,axis='columns')
        
        # if apple detected, will be the second url (appending order in _get_urls)
        elif self.apple_detected:
            self.apple_detected = False
            df = partial_requested_data[partial_requested_data.geo_type=='sub-region']
            df = df[df.region.isin(region_english_names)]
            df.region.replace(to_replace=english_spanish_dict,inplace=True)
            df.drop(['geo_type','alternative_name','sub-region','country'],axis='columns',errors='ignore',inplace=True)
            df.set_index(['region','transportation_type'], drop=True, inplace=True)
            df = df.transpose()
        
        else:
            # never should get there
            return None
            
        df.columns.rename("Item",level=1,inplace=True)
        df.columns.rename("Region",level=0,inplace=True)
        df.set_index(pd.to_datetime(df.index, format="%Y-%m-%d"),inplace=True)
        return df
