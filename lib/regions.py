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
            Country of the regions. Up to now, only 'ES' for Spanish provinces is available.

        Returns
        -------
            list of str
                A list with the names of the Spanish provinces.
        """

        if country_code not in cls.get_country_codes():
            print("Country not implemented yet!")
            return None

        # first time using Regions, read configuration of REGIONS
        if cls.__REGION_CONFIGURATION is None:
            loaded = cls.__load_region_configuration(country_code)
            if not loaded:
                return None

        return list(cls.__REGION_CONFIGURATION.keys())

    @classmethod
    def get_regions_by_type(cls, type='r', country_code='ES'):
        """
        Gets the implemented regions for a specific country code.

        Parameters
        ----------
        type: str
            Region type. 'r' Region, 'p' Province.
        country_code: str
            Country of the regions. Up to now, only 'ES' for Spanish provinces is available.

        Returns
        -------
            list of str
                A list with the names of the Spanish provinces.
        """

        if country_code not in cls.get_country_codes():
            print("Country not implemented yet!")
            return None

        # first time using Regions, read configuration of REGIONS
        if cls.__REGION_CONFIGURATION is None:
            loaded = cls.__load_region_configuration(country_code)
            if not loaded:
                return None

        list_by_type = []

        for i in cls.__REGION_CONFIGURATION.keys():
            if type == 'r' and 'CA' in i:
                list_by_type.append(cls.__REGION_CONFIGURATION[i]['nombre'])  # cls.__REGION_CONFIGURATION[i]['nombre']
            elif type == 'p' and 'CA' not in i and int(cls.__REGION_CONFIGURATION[i]['code_ine']) is not 0:
                list_by_type.append(cls.__REGION_CONFIGURATION[i]['nombre'])

        return list_by_type

    @classmethod
    def get_regions_population(cls, country_code='ES'):

        if country_code not in cls.get_country_codes():
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
        list of str
            a list with the supported country codes. Up to now, only 'ES' for Spanish provinces is available. 
        """

        COUNTRIES = ['ES']
        return COUNTRIES

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
            Country of the regions. Up to now, only 'ES' for Spanish provinces is available.

        Returns
        -------
            list of str
                A list of the region representations in the same order as provided in regions.
        """

        # first time using Regions, read configuration of REGIONS
        if cls.__REGION_CONFIGURATION is None:
            loaded = cls.__load_region_configuration(country_code)
            if not loaded:
                return None

        # get possible region representations
        possible_region_representations = cls.__REGION_CONFIGURATION[list(cls.__REGION_CONFIGURATION.keys())[0]].keys()

        # check parameters
        if region_representation not in possible_region_representations:
            print("ERROR: Region representation not found")
            return None

        # country detection
        country = False
        c = 0
        while not country and c < len(cls.get_country_codes()):
            country = all(region in cls.get_regions(cls.get_country_codes()[c]) for region in regions)
            c = c + 1

        # all regions are implemented: properties extraction
        if country:
            country = c - 1

            properties = []
            for region in regions:
                prop = cls.__REGION_CONFIGURATION[region].get(region_representation)
                properties.append(prop)
            return properties
        else:
            print("ERROR: All or some regions are not implemented.")
            return None

    # private

    @classmethod
    def __load_region_configuration(cls, country_code='ES'):
        """
        Reads and loads the configuration file of a country.

        Parameters
        ----------
        country_code: str
            Country of the regions. Up to now, only 'ES' for Spanish provinces is available.
        
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
            print("ERROR: Configuration file of " + str(country_code) + "region  not found!")
            return False
        except json.JSONDecodeError as e:
            print("ERROR: Configuration file of " + str(country_code) + "region is not well built!", e)
            return False
        return True
