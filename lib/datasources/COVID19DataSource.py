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
        template_url = 'https://raw.githubusercontent.com/datadista/datasets/master/COVID%2019/ccaa_covid19_{name}.csv'

        index_data_item = 0
        for data_item in self.data_items:
            try:
                data_source = self.__class__.DATA_ITEMS_INFO[self.data_items[index_data_item]]['data_source']
            except KeyError:
                data_source = None

            if data_source != None and data_source == "ISCIII":
                if not self.isciii_detected:
                    self.isciii_detected = True
                index_data_item += 1
                continue
            if data_item == "fallecidos" or data_item == 'accumulated_fallecidos' or data_item == 'fallecidos_100k' or data_item == 'accumulated_lethality':
                if not self.fallecidos_detected:
                    self.fallecidos_detected = True
                index_data_item += 1
                continue

            url = template_url.format(name=data_item)
            urls.append(url)
            index_data_item += 1

        if self.isciii_detected:
            urls.append(
                'https://raw.githubusercontent.com/datadista/datasets/master/COVID%2019/ccaa_covid19_datos_isciii_nueva_serie.csv')

        if self.fallecidos_detected:
            urls.append(
                'https://raw.githubusercontent.com/datadista/datasets/master/COVID%2019/ccaa_covid19_fallecidos_por_fecha_defuncion_nueva_serie.csv')

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

        region_population_dict = Regions._get_property(self.regions, "population")
        norm_region_population_dict = dict(zip(self.regions, region_population_dict))

        self.isciii_detected = True if 'fecha' in partial_requested_data.columns else False
        df = partial_requested_data.rename(columns={"cod_ine": "Region"})
        df.set_index(df.Region.astype(str).str.zfill(2), inplace=True,
                     drop=True)  # zfill used to change numbers 1, 2, 3... tu padded strings "01", "02"... (code ine)

        if not self.isciii_detected:
            '''
            PROCESSING METHOD FOR OLD DATADISTA SERIES
            '''
            # improve after
            data_item_selected = [s for s in [r for data_item in self.data_items for r in data_item.split('_')] if s in self.processing_url]
            df = df.drop(['Region', 'CCAA'], axis='columns', errors='ignore')
            df = df.transpose()
            df = df[region_representation_dict]  # filter regions
            df.rename(columns=representation_region_dict,
                      inplace=True)  # replace region representation by region itself
            df = df.astype('float64')
            df.columns = pd.MultiIndex.from_product([df.columns, [data_item_selected[0]]])
            df.set_index(pd.to_datetime(df.index, format="%Y-%m-%d"), inplace=True)
            df.columns.rename("Item", level=1, inplace=True)

            '''
            if it is the "fallecidos" dataitem, the accumulated and 100k series is created and added to the final table
            '''
            if data_item_selected[0] == 'fallecidos':
                for (columnName, columnData) in df.iteritems():
                    df[columnName[0], 'accumulated_fallecidos'] = df[columnName[0], 'fallecidos'].cumsum()
                    df[columnName[0], 'fallecidos_100k'] = df.apply(
                        lambda x: (x[columnName[0], 'accumulated_fallecidos'] * 100000) / float(norm_region_population_dict.get(columnName[0])) if columnName[0] in norm_region_population_dict.keys() else 0, axis=1)

        elif self.isciii_detected:
            '''
            PROCESSING METHOD FOR NEW ISCIII SERIE
            '''
            self.isciii_detected = False
            df['fecha'] = pd.to_datetime(df['fecha'], format='%Y-%m-%d')
            df.sort_values(['fecha'], inplace=True)
            df = df.drop(
                ['Region', 'CCAA', 'ccaa', 'num_casos_prueba_otras', 'num_casos_prueba_desconocida'], axis='columns',
                errors='ignore')

            df['accumulated_cases'] = df.groupby(df.index)['num_casos'].cumsum()
            df['accumulated_cases_pcr'] = df.groupby(df.index)['num_casos_prueba_pcr'].cumsum()
            df['accumulated_cases_test'] = df.groupby(df.index)['num_casos_prueba_test_ac'].cumsum()

            df = df.astype({'num_casos': 'float64'})
            df.rename(index=representation_region_dict, inplace=True)
            df['cases_100k'] = df.apply(lambda x: (x['accumulated_cases'] * 100000) / float(
                norm_region_population_dict.get(x.name)) if x.name in norm_region_population_dict.keys() else 0, axis=1)
            df = df.pivot_table(index='fecha', columns='Region').swaplevel(i=0, j=1, axis='columns')
            df.columns.rename("Item", level=1, inplace=True)
            df.set_index(pd.to_datetime(df.index, format="%Y-%m-%d"), inplace=True)

        return df
