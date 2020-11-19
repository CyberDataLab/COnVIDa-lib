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

        self.isciii_detected = False
        self.fallecidos_detected = False

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
        urls.append(
            'https://raw.githubusercontent.com/montera34/escovid19data/master/data/output/covid19-spain_consolidated.csv')
        urls.append(
            'https://raw.githubusercontent.com/montera34/escovid19data/master/data/output/covid19-ccaa-spain_consolidated.csv')
        urls.append(
            'https://raw.githubusercontent.com/montera34/escovid19data/master/data/output/covid19-provincias-spain_consolidated.csv')

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

        representation_ccaa_dict = {}
        representation_provinces_dict = {}
        representation_spain_dict = {'00': 'España'}
        for i, r in enumerate(self.regions):
            code_ine = region_representation_dict[i]
            if 'CA' in r or code_ine == 0:
                representation_ccaa_dict[code_ine] = r
            else:
                representation_provinces_dict[code_ine] = r

        # Solution for esCOVID19data error
        if "intensive_care_per_1000000" in partial_requested_data.columns:
            partial_requested_data = partial_requested_data.rename(
                columns={'intensive_care_per_1000000': 'intensive_care_per_100000'})

        if "ine_code" not in partial_requested_data.columns:
            partial_requested_data.insert(1, 'ine_code', 0)

        df = partial_requested_data.rename(columns={"ine_code": "Region"})
        df.set_index(df.Region.astype(str).str.zfill(2), inplace=True,
                     drop=True)  # zfill used to change numbers 1, 2, 3... tu padded strings "01", "02"... (code ine)

        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
        df.sort_values(['date'], inplace=True)
        df = df.drop(['Region', 'CCAA', 'ccaa', 'province', 'source_name', 'source', 'comments'], axis='columns',
                     errors='ignore')

        if "province" in partial_requested_data.columns:
            df.rename(index=representation_provinces_dict, inplace=True)
        elif "ccaa" in partial_requested_data.columns:
            df.rename(index=representation_ccaa_dict, inplace=True)
        else:
            df.rename(index=representation_spain_dict, inplace=True)

        df = df.pivot_table(index='date', columns='Region').swaplevel(i=0, j=1, axis='columns')
        df.columns.rename("Item", level=1, inplace=True)
        df.set_index(pd.to_datetime(df.index, format="%Y-%m-%d"), inplace=True)

        return df
