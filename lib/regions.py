import json
import os


class Regions(object):
    """ 
    Wrapper class to manage region information

    Attributes
    ----------
    __REGION_CONFIGURATION : dict
        Information of the regions
    __CONFIG_PATH : str
        Relative ath to the region configuration file 
    """

    __REGION_CONFIGURATION = None
    __CONFIG_PATH = 'config'

    # public
    @classmethod
    def get_regions(cls, country_code='ES'):
        """
        Gets the implemented regions for a specific country code.

        Parameters
        ----------
        country_code: str
            Country of the regions. Up to now, only 'ES' for Spanish regions is available.

        Returns
        -------
            list of str
                A list with the names of the Spanish regions.
        """

        if country_code not in cls.get_country_codes().values():
            print("Country not implemented yet!")
            return None

        # first time using Regions, read configuration of REGIONS
        if cls.__REGION_CONFIGURATION is None:
            loaded = cls.__load_region_configuration(country_code)
            if not loaded:
                return None

        return list(cls.__REGION_CONFIGURATION.keys())

    @classmethod
    def get_regions_by_type(cls, type='c', country_code='ES'):
        """
        Gets the implemented regions for a specific country code.

        Parameters
        ----------
        type: str
            Region type. For Spain (ES), 'c' is Community, 'p' is Province.
        country_code: str
            Country of the regions. 

        Returns
        -------
        list of str
            A list with the names of the Spanish regions.
        """

        if country_code not in cls.get_country_codes().values():
            print("Country not implemented yet!")
            return None

        # first time using Regions, read configuration of REGIONS
        if cls.__REGION_CONFIGURATION is None:
            loaded = cls.__load_region_configuration(country_code)
            if not loaded:
                return None

        list_by_type = []

        for i in cls.__REGION_CONFIGURATION.keys():
            if type == 'c' and 'CA' in i:
                list_by_type.append(cls.__REGION_CONFIGURATION[i]['nombre'])  # cls.__REGION_CONFIGURATION[i]['nombre']
            elif type == 'p' and 'CA' not in i and int(cls.__REGION_CONFIGURATION[i]['code_ine']) is not 0:
                list_by_type.append(cls.__REGION_CONFIGURATION[i]['nombre'])

        return list_by_type

    @classmethod
    def get_regions_population(cls, country_code='ES'):
        """
        Returns the number of citizens per region in a specific country

        Parameters
        ----------
        country_code: str
            Country of the regions. Up to now, only 'ES' for Spanish regions is available.

        Returns
        -------
        dict { string : int }
            A dictionary with regions as keys, population as values
        """
        if country_code not in cls.get_country_codes().values():
            print("Country not implemented yet!")
            return None

        # first time using Regions, read configuration of REGIONS
        if cls.__REGION_CONFIGURATION is None:
            loaded = cls.__load_region_configuration(country_code)
            if not loaded:
                return None

        regions_with_population = {}

        for i in cls.__REGION_CONFIGURATION.keys():
            regions_with_population[i] = int(cls.__REGION_CONFIGURATION[i]['population'])

        return regions_with_population

    
    @classmethod
    def get_country_codes(cls):
        """
        Gets the implemented country codes.
        
        Returns
        -------
        dict { str : str }
            a dictionary with countries as keys, and codes as values
        """
        current_path = os.path.dirname(os.path.realpath(__file__))
        config_path = os.path.join(current_path, cls.__CONFIG_PATH)
        with open(os.path.join(config_path, "countries.json"), encoding="utf8") as countries:
            countries_json = json.load(countries)
        
        country_codes = {}
        for country in countries_json.keys():
            code = countries_json[country]['country_code']
            country_codes[country] = code
        
        return country_codes

    # protected for only Data Source classes

    @classmethod
    def _get_property(cls, regions, region_representation, country_code='ES'):

        """
        Gets the internal region representation of specific regions.

        Parameters
        ----------
        regions: list
            List of region names
        region_representation: str
            Name of the region representation to be queried, namely 'nombre', 'iso_3166_2', 'literal_ine', 'code_ine', 'name' or 'aemet_stations'.
        country_code: str
            Country code of the regions.

        Returns
        -------
            list of str
                A list of the region representations in the same order as provided in regions.
        """
        
        country_codes = cls.get_country_codes()
        
        if country_code not in country_codes.values():
            print("Country not implemented yet!")
            return None
        
        # first time using Regions, read configuration of REGIONS
        if cls.__REGION_CONFIGURATION is None:
            loaded = cls.__load_region_configuration(country_code)
            if not loaded:
                return None
        
        # get country associated to code
        country = None
        for c in country_codes.keys():
            if country_codes[c] == country_code:
                country = c
                
        if country is None:
            print("Country not found!")
            return None
        
        # get possible region representations
        possible_region_representations = cls.__REGION_CONFIGURATION[country]['region_representations']
        
        # check parameters
        if region_representation not in possible_region_representations:
            print("ERROR: Region representation not found")
            return None

        # properties extraction
        properties=[]
        for region in regions:
            prop = cls.__REGION_CONFIGURATION[region].get(region_representation)
            properties.append(prop)
            
        return properties


    # private

    @classmethod
    def __load_region_configuration(cls, country_code='ES'):
        """
        Reads and loads the configuration file of a country.

        Parameters
        ----------
        country_code: str
            Country of the regions.
        
        Returns
        -------
        boolean
            True if the load worked, False otherwise.

        """

        ## read general info of the data source
        current_path = os.path.dirname(os.path.realpath(__file__))
        config_path = os.path.join(current_path, cls.__CONFIG_PATH)
        # config_file = os.path.join(config_path,country_code+'-regions.json')
        json_files = [pos_json for pos_json in os.listdir(config_path) if pos_json.endswith('.json')]

        try:
            json_list = {}
            for i in json_files:
                with open(os.path.join(config_path, i), encoding="utf8") as json_file:
                    data = json.load(json_file)
                    json_list.update(data)

            cls.__REGION_CONFIGURATION = json_list

        except FileNotFoundError as e:
            print("ERROR: Configuration files of " + str(country_code) + "regions not found!")
            return False
        except json.JSONDecodeError as e:
            print("ERROR: Configuration files of " + str(country_code) + "regions not well built!", e)
            return False
        return True
