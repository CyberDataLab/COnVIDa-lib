from .datasource import DataSource
import pandas as pd
from regions import Regions


class MoMoDataSource(DataSource):
    """ 
    Class which constitutes a collection from the MoMo Data Source.

    Attributes
    ----------
    DATA_TYPE : DataType
        Data Type of the Data Source (TEMPORAL or GEOGRAPHICAL)
    TEMPORAL_GRANULARITY : list of TemporalGranularity
        List of temporal units of the time series (DAILY)
    REGIONAL_GRANULARITY : list of RegionalGranularity
        List of regional units of the data series (COMMUNITY or/and PROVINCE)
    REGION_REPRESENTATION : str
        Representation of the regions within the Data Source (iso_3166_2, ine code, ...)
    DATA_FORMAT : DataFormat
        Data Format of the resource (JSON or CSV)
    UPDATE_FREQUENCY : int
        Period, in days, taken by the official repository to update the data series.
    DATA_ITEMS : list of str
        Names of the data items literally used by the Data Source
    DATA_ITEMS_INFO : dic { str : dic { str : dic { str : str } } }
        Information of each Data Item of the Data Source. The internal name (DATA_ITEMS) are the keys, whereas the following aspects are the keys of the first nested dic.
            Display Name (used to change the third-party nomenclature to a desired custom one)
            Description (meaning of the Data Item)
            Data unit (metric of the Data Item values: kg, persons, etc.)
        The second nested dic correspond to the keys 'EN' and 'ES', containing the English and Spanish texts respectively.
    """
    
    DATA_TYPE = None
    TEMPORAL_GRANULARITY = None
    REGIONAL_GRANULARITY = None
    REGION_REPRESENTATION = None
    DATA_FORMAT = None
    UPDATE_FREQUENCY = None
    DATA_ITEMS = None
    DATA_ITEMS_INFO = None
    
    
    def __init__(self, data_items=None, regions=None, start_date=None, end_date=None):
        """
        Creates a collection of the MoMo Data Source

        Parameters
        ----------
        data_items : list of str
            list of required data item names of the MoMo Data Source.
        regions : list of str
            list of required regions. 
        start_date : pd.datetime
            first day to be considered. 
        end_date : pd.datetime
            last day to be considered. 
        
        Returns
        -------
        Returns a configured MoMo Data Source object ready to be used for getting the data.

        Notes
        -----
        By default, parameters to None indicates an empty instance (used to initialize class attributes).
        """

        super().__init__(data_items,regions)
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
        url = 'https://momo.isciii.es/public/momo/data'
        return [url]
    
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
        region_codes_ine = Regions._get_property(self.regions, self.__class__.REGION_REPRESENTATION)
        codesine_regions_dict = dict(zip(region_codes_ine,self.regions))
        
        df = partial_requested_data[(partial_requested_data['cod_ine_ambito'].isin(region_codes_ine)) & (partial_requested_data['nombre_sexo'] == 'todos') & (partial_requested_data['nombre_gedad'] == 'todos')]
        df.cod_ine_ambito = df.cod_ine_ambito.astype(int).astype(str).apply(lambda x: x.zfill(2)).replace(codesine_regions_dict)
        df.rename(columns={'cod_ine_ambito':'Region'},inplace=True)
        df.drop(['ambito','cod_ambito','nombre_ambito','cod_sexo','nombre_sexo','cod_gedad','nombre_gedad'],axis='columns',errors='ignore',inplace=True)
        df = df[['Region','fecha_defuncion']+self.data_items]
        df.set_index(['Region'],inplace=True)
        df = df.pivot_table(index='fecha_defuncion',columns='Region').swaplevel(i=0,j=1,axis='columns')
        df.columns.rename("Item",level=1,inplace=True)
        df.set_index(pd.to_datetime(df.index, format="%Y-%m-%d"),inplace=True)  
        return df
