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
        # urls.append(
        #     'https://raw.githubusercontent.com/montera34/escovid19data/master/data/output/covid19-spain_consolidated.csv')
        urls.append(
            'https://raw.githubusercontent.com/montera34/escovid19data/master/data/output/covid19-ccaa-spain_consolidated.csv')
        urls.append(
            'https://raw.githubusercontent.com/montera34/escovid19data/master/data/output/covid19-provincias-spain_consolidated.csv')
        urls.append(
            'https://raw.githubusercontent.com/montera34/escovid19data/master/data/original/vacunas/estado_vacunacion_.csv')
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
        region_population_dict = Regions.get_regions_population()

        representation_ccaa_dict = {}
        representation_provinces_dict = {}
        representation_spain_dict = {'00': 'España'}
        for i, r in enumerate(self.regions):
            code_ine = region_representation_dict[i]
            if 'CA' in r or code_ine == 0:
                representation_ccaa_dict[code_ine] = r
            else:
                representation_provinces_dict[code_ine] = r

        # Adaptation to vaccines dataset
        representation_ccaa_vac_dict = {'Totales': 'España', 'Andalucía': 'CA Andalucía', 'Aragón': 'CA Aragón',
                                        'Asturias': 'CA Principado de Asturias',
                                        'Baleares': 'CA Islas Baleares', 'Canarias': 'CA Canarias',
                                        'Cantabria': 'CA Cantabria', 'Castilla y Leon': 'CA Castilla y León',
                                        'Castilla La Mancha': 'CA Castilla-La Mancha',
                                        'Cataluña': 'CA Cataluña', 'C. Valenciana': 'CA Comunidad Valenciana',
                                        'Extremadura': 'CA Extremadura', 'Galicia': 'CA Galicia',
                                        'Madrid': 'CA Comunidad de Madrid', 'Murcia': 'CA Región de Murcia',
                                        'Navarra': 'CA Comunidad Foral de Navarra', 'País Vasco': 'CA País Vasco',
                                        'La Rioja': 'CA La Rioja', 'Ceuta': 'CA Ceuta', 'Melilla': 'CA Melilla'}

        # Fix esCOVID19data error
        if "intensive_care_per_1000000" in partial_requested_data.columns:
            partial_requested_data = partial_requested_data.rename(
                columns={'intensive_care_per_1000000': 'intensive_care_per_100000'})

        # Vaccine
        if "date_pub" in partial_requested_data.columns:
            df = partial_requested_data.rename(
                columns={"ccaa": "Region", 'date_pub': 'date', 'Dosis entregadas': 'vaccine_provided',
                         'Dosis administradas': 'vaccine_supplied',
                         '% sobre entregadas': 'vaccine_supplied_inc'})
            df['Region'] = df['Region'].map(representation_ccaa_vac_dict)

            df['date'] = pd.to_datetime(df['date'], dayfirst=True)
            df.set_index(df.Region.astype(str).str.zfill(2), inplace=True, drop=True)

        else:
            df = partial_requested_data.rename(columns={"ine_code": "Region"})
            df.set_index(df.Region.astype(str).str.zfill(2), inplace=True,
                         drop=True)  # zfill used to change numbers 1, 2, 3... tu padded strings "01", "02"... (code ine)

        print(df.columns)
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
        df.sort_values(['date'], inplace=True)
        df = df.drop(['Region', 'CCAA', 'ccaa', 'province', 'source_name', 'source', 'comments'], axis='columns',
                     errors='ignore')

        # Adaptation of dataitems
        if "province" in partial_requested_data.columns:
            df.rename(index=representation_provinces_dict, inplace=True)
        elif "ccaa" in partial_requested_data.columns and "date_pub" not in partial_requested_data.columns:
            df.rename(index=representation_ccaa_dict, inplace=True)

        df = df.pivot_table(index='date', columns='Region').swaplevel(i=0, j=1, axis='columns')
        df.columns.rename("Item", level=1, inplace=True)
        df.set_index(pd.to_datetime(df.index, format="%Y-%m-%d"), inplace=True)

        if "date_pub" in partial_requested_data.columns:
            for i in df.columns.levels[0]:
                df[i, 'pob_vaccine_supplied_inc'] = ((df[i, 'vaccine_supplied'] * 100) / region_population_dict[i]).round(2)
                df[i, 'vaccine_supplied_inc'] = df[i, 'vaccine_supplied_inc'] * 100

        if ("ccaa" in partial_requested_data.columns or "province" in partial_requested_data.columns) and "date_pub" not in partial_requested_data.columns:
            for i in df.columns.levels[0]:
                df[i, 'accumulated_lethality'] = (df[i, 'deceased'] / df[i, 'cases_accumulated']).round(5)
                df[i, 'daily_deaths_inc'] = df[i, 'daily_deaths_inc'] * 100

        if "ccaa" in partial_requested_data.columns and "date_pub" not in partial_requested_data.columns:
            # Adaptation of Spain region
            try:
                sum_dataitems = df.sum(axis=1, level=1)
                df['España', 'num_casos'] = sum_dataitems['num_casos']
                df['España', 'num_casos_prueba_pcr'] = sum_dataitems['num_casos_prueba_pcr']
                df['España', 'num_casos_prueba_test_ac'] = sum_dataitems['num_casos_prueba_test_ac']
                df['España', 'num_casos_prueba_ag'] = sum_dataitems['num_casos_prueba_ag']
                df['España', 'num_casos_prueba_elisa'] = sum_dataitems['num_casos_prueba_elisa']
                df['España', 'num_casos_prueba_desconocida'] = sum_dataitems['num_casos_prueba_desconocida']
                df['España', 'daily_deaths'] = sum_dataitems['daily_deaths']

                df['España', 'cases_accumulated'] = sum_dataitems['cases_accumulated']
                df['España', 'cases_accumulated_PCR'] = sum_dataitems['cases_accumulated_PCR']
                df['España', 'hospitalized'] = sum_dataitems['hospitalized']
                df['España', 'intensive_care'] = sum_dataitems['intensive_care']
                df['España', 'deceased'] = sum_dataitems['deceased']
                df['España', 'recovered'] = sum_dataitems['recovered']

                # Ventanas de tiempo
                df['España', 'daily_deaths_avg7'] = sum_dataitems['daily_deaths_avg7']
                df['España', 'cases_14days'] = sum_dataitems['cases_14days']

                # Medias

                def media_by_param(df, param, out, days):
                    df['España', out] = 0
                    cont = 0
                    datainv = df.reindex(index=df.index[::-1])
                    for i, idx in enumerate(datainv.index):
                        valor = 0
                        for idxx in datainv.index[i:]:
                            if cont < days:
                                valor += df.loc[idxx, ('España', param)]
                                cont = cont + 1
                        media = valor / days
                        df.loc[idx, ('España', out)] = media
                        cont = 0

                media_by_param(df, 'num_casos', 'daily_cases_avg7', 7)
                media_by_param(df, 'num_casos_prueba_pcr', 'num_casos_prueba_pcr_avg7', 7)
                media_by_param(df, 'daily_deaths', 'daily_deaths_avg7', 7)
                media_by_param(df, 'daily_deaths', 'daily_deaths_avg3', 3)

                # IA
                def ia_by_param(df, param, out, days):
                    df['España', out] = 0
                    cont = 0
                    datainv = df.reindex(index=df.index[::-1])
                    for i, idx in enumerate(datainv.index):
                        valor = 0
                        for idxx in datainv.index[i:]:
                            if cont < days:
                                valor += df.loc[idxx, ('España', param)]
                                cont = cont + 1
                        ia = ((valor * 100000) / region_population_dict['España']).round(2)
                        df.loc[idx, ('España', out)] = ia
                        cont = 0

                ia_by_param(df, 'num_casos', 'ia14', 14)

                # Lethality

                df['España', 'accumulated_lethality'] = (
                        df['España', 'deceased'] / df['España', 'cases_accumulated']).round(2)

                # 100k

                df['España', 'cases_per_cienmil'] = ((df['España', 'cases_accumulated'] * 100000) / region_population_dict['España']).round(2)
                df['España', 'intensive_care_per_100000'] = (
                            (df['España', 'intensive_care'] * 100000) / region_population_dict['España']).round(2)
                df['España', 'hospitalized_per_100000'] = ((df['España', 'hospitalized'] * 100000) / region_population_dict['España']).round(2)
                df['España', 'deceassed_per_100000'] = ((df['España', 'deceased'] * 100000) / region_population_dict['España']).round(2)

                # percent

                def percent_by_param(df, param, out, days):
                    df['España', out] = 0
                    cont = 0
                    datainv = df.reindex(index=df.index[::-1])
                    for i, idx in enumerate(datainv.index):
                        valor = 0
                        for idxx in datainv.index[i + 1:]:
                            if cont < days:
                                valor += df.loc[idxx, ('España', param)]
                                cont = cont + 1
                        percent = (df.loc[idx, ('España', param)] * 100) / valor
                        df.loc[idx, ('España', out)] = percent.round(2)
                        cont = 0

                percent_by_param(df, 'daily_deaths', 'daily_deaths_inc', 1)

            except KeyError as e:
                print("Spain dataitems ERROR: ", e)

        return df
