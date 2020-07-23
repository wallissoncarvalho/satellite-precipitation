"""
Created on Wed Jul 22 22:27:09 2020
@author: wallissoncarvalho
"""

from nasadap import Nasa, parse_nasa_catalog
import json


# Parameters Definition
credentials = json.loads(open(".earthdata_credentials").read())
product_summary = {'mission': 'gpm', 'product': '3IMERGHH', 'version': 6, 'dataset_type': 'precipitationCal'}
dates = {'from': None, 'to': None}

# Coordinates Definition
coordinates = {'min_lat': -33, 'max_lat': 3, 'min_lon': -72, 'max_lon': -35}

# Getting the Initial and Final dates
min_max = parse_nasa_catalog(product_summary['mission'], product_summary['product'], product_summary['version'],
                             min_max=True)


# Dates definition 
dates['from'] = min_max.from_date.to_list()[0].strftime("%Y-%m-%d")
dates['to'] = min_max.to_date.to_list()[-1].strftime("%Y-%m-%d")

# Accessing Nasa Available Products
nasa_access = Nasa(credentials['username'], credentials['password'], product_summary['mission'], cache_dir=r'prod_data')

# Downloading Data
dataset = nasa_access.get_data(product_summary['product'], product_summary['version'], product_summary['dataset_type'],
                               dates['from'], dates['to'], coordinates['min_lat'], coordinates['max_lat'],
                               coordinates['min_lon'], coordinates['max_lon'])


nasa_access.close()
