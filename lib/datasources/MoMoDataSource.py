from .datasource import DataSource
import pandas as pd
from regions import Regions


class MoMoDataSource(DataSource):
    
    DATA_FORMAT = None
    DATA_TYPE = None
    REGION_REPRESENTATION = None
    DATA_ITEMS = None
    DATA_ITEMS_INFO = None
    
    
    def __init__(self, data_items=None, regions=None, start_date=None, end_date=None):
        super().__init__(data_items,regions)
        self.start_date = start_date
        self.end_date = end_date

    # defined methods from parent class

    def _get_urls(self):
        url = 'https://momo.isciii.es/public/momo/data'
        return [url]
    
    def _manage_response(self, response):
        return response

    def _process_partial_data(self, partial_requested_data):
        
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
