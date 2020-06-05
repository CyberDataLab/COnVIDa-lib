import json
import os 

# Wrapper class to handle regions
class Regions(object):
    
    __REGION_CONFIGURATION = None
    __CONFIG_PATH = 'config'
    
    # public
    @classmethod
    def get_regions(cls, country_code='ES'):
        
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
    def get_country_codes(cls):
        COUNTRIES = ['ES']
        return COUNTRIES
    
    # protected for only Data Item factory
    
    @classmethod
    def _get_property(cls, regions, region_representation, country_code='ES'):
        
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
            c = c+1
           
        
        # all regions are implemented: properties extraction
        if country:    
            country = c-1
            
            country_regions = cls.get_regions(cls.get_country_codes()[country])
            
            properties = []
            for region in regions:
                prop = cls.__REGION_CONFIGURATION[region].get(region_representation)
                properties.append(prop)
            return properties
        else:
            print("ERROR: All or some regions are not implemented.")
            return None
    

    #private
    
    @classmethod  
    def __load_region_configuration(cls, country_code='ES'):
        
        ## read general info of the data source
        current_path = os.path.dirname(os.path.realpath(__file__))
        config_path = os.path.join(current_path,cls.__CONFIG_PATH)
        config_file = os.path.join(config_path,country_code+'-regions.json')
        
        try:
            with open(config_file,  encoding="utf8") as json_file:
                    cls.__REGION_CONFIGURATION = json.load(json_file)
        except FileNotFoundError as e:
            print("ERROR: Configuration file of " + str(country_code) + "region  not found!")
            return False
        except json.JSONDecodeError as e:
            print("ERROR: Configuration file of " + str(country_code) + "region is not well built!", e)
            return False
        return True
    