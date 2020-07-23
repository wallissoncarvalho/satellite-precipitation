"""
Created on Wed Jul 22 22:27:09 2020
@author: wallissoncarvalho

This file is based on https://github.com/mullenkamp/nasadap
"""

mission_product_dict = {
    'gpm': {
        'base_url': 'https://gpm1.gesdisc.eosdis.nasa.gov:443',
        'process_level': 'GPM_L3',
        'version': 6,
        'products': {
            '3IMERGHHE': '{mission}_{product}.{version:02}/{year}/{dayofyear:03}/3B-HHR-E.MS.MRG.3IMERG.{'
                         'date}-S{time_start}-E{time_end}.{minutes}.V{version:02}B.HDF5',
            '3IMERGHHL': '{mission}_{product}.{version:02}/{year}/{dayofyear:03}/3B-HHR-L.MS.MRG.3IMERG.{'
                         'date}-S{time_start}-E{time_end}.{minutes}.V{version:02}B.HDF5',
            '3IMERGHH': '{mission}_{product}.{version:02}/{year}/{dayofyear:03}/3B-HHR.MS.MRG.3IMERG.{'
                        'date}-S{time_start}-E{time_end}.{minutes}.V{version:02}B.HDF5 '
        }
    }
}

master_datasets = {'3IMERGHHE': ['precipitationQualityIndex', 'IRkalmanFilterWeight', 'precipitationCal',
                                 'HQprecipitation', 'probabilityLiquidPrecipitation', 'randomError', 'IRprecipitation'],
                   '3IMERGHHL': ['precipitationQualityIndex', 'IRkalmanFilterWeight', 'precipitationCal',
                                 'HQprecipitation', 'probabilityLiquidPrecipitation', 'randomError', 'IRprecipitation'],
                   '3IMERGHH': ['precipitationQualityIndex', 'IRkalmanFilterWeight', 'precipitationCal',
                                'HQprecipitation', 'probabilityLiquidPrecipitation', 'randomError', 'IRprecipitation']}
