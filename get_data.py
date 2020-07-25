"""
Created on Wed Jul 22 22:27:09 2020
@author: wallissoncarvalho

This file is based on https://github.com/mullenkamp/nasadap
"""
import os
import pandas as pd
import numpy as np
import xarray as xr
import requests
from time import sleep
from lxml import etree
import itertools
from multiprocessing.pool import ThreadPool
from pydap.cas.urs import setup_session
from pydap.client import open_url
from base import mission_product_dict, master_datasets


class Nasa:
    """
    Class to download, select, and convert NASA data via opendap.

    Parameters
    ----------
    username : str
        The username for the login.
    password : str
        The password for the login.
    mission : str
        Mission name.
    cache_dir : str or None
        A path to cache the netcdf files for future reading. If None, the currently working directory is used.

    Returns
    -------
    Nasa object
    """
    missions_products = {m: list(mission_product_dict[m]['products'].keys()) for m in mission_product_dict}

    def __init__(self, username, password, mission, product, version=6, cache_dir=None):
        """
        Initiate the object with a dap session.

        Parameters
        ----------
        username : str
            The username for the login.
        password : str
            The password for the login.
        mission : str
            Mission name.
        product : str
            Data product associated with the mission.
        version: int, optional
            Data product version.
        cache_dir : str or None
            A path to cache the netcdf files for future reading. If None, the currently working directory is used.

        Returns
        -------
        Nasa object
        """
        # Verifying the mission
        if mission in mission_product_dict:
            self.mission_dict = mission_product_dict[mission]
        else:
            raise ValueError('Mission should be one of: ' + ', '.join(mission_product_dict.keys()))

        self.mission = mission

        # Verifying the product
        if product in self.mission_dict['products']:
            self.product = product
        else:
            raise ValueError('Product must be one of: ' + ', '.join(self.mission_dict['products'].keys()))

        # Defining Version
        self.version = version

        # Verifying cache_dir
        if isinstance(cache_dir, str):
            if not os.path.exists(cache_dir):
                os.makedirs(cache_dir)
            self.cache_dir = cache_dir
        else:
            self.cache_dir = os.getcwd()

        # Setting up a pydap session
        self.session = setup_session(username, password, check_url='/'.join(
            [self.mission_dict['base_url'], 'opendap', self.mission_dict['process_level']]))

    def __get_files_urls(self, date, file_path, base_url):
        path = file_path.format(mission=self.mission.upper(), product=self.product, year=date.year,
                                dayofyear=date.dayofyear, version=self.version)
        path = '/'.join([self.mission_dict['process_level'], path])
        url = '/'.join([base_url, 'opendap', path, 'catalog.xml'])
        response = requests.get(url)
        et = etree.fromstring(response.content)
        urls = [base_url + c.attrib['ID'] for c in et.getchildren()[3].getchildren() if '.xml' not in c.attrib['ID']]
        return urls

    def __download_files(self, url, path, datasets, min_lat, max_lat, min_lon, max_lon):

        # Setting up coordinates
        range_lon = np.round(np.arange(-179.95, 180, 0.1), 2)
        range_lat = np.round(np.arange(-89.95, 90, 0.1), 2)
        min_lon = (np.abs(range_lon - min_lon)).argmin()
        max_lon = (np.abs(range_lon - max_lon)).argmin()
        min_lat = (np.abs(range_lat - min_lat)).argmin()
        max_lat = (np.abs(range_lat - max_lat)).argmin()

        if min_lon >= max_lon:
            raise ValueError('min_lon must be smaller than max_lon')
        if min_lat >= max_lat:
            raise ValueError('min_lat must be smaller than max_lat')

        coordinates = '[0:1:0][{min_lon}:1:{max_lon}][{min_lat}:1:{max_lat}]'.format(min_lon=min_lon, max_lon=max_lon,
                                                                                     min_lat=min_lat, max_lat=max_lat)

        #Update url
        url += '?'
        if 'precipitationQualityIndex' in datasets:
            url += 'precipitationQualityIndex'+coordinates + ','
        if 'IRkalmanFilterWeight' in datasets:
            url += 'IRkalmanFilterWeight' + coordinates + ','
        if 'HQprecipSource' in datasets:
            url += 'HQprecipSource' + coordinates + ','
        if 'precipitationCal' in datasets:
            url += 'precipitationCal' + coordinates + ','
        if 'precipitationUncal' in datasets:
            url += 'precipitationUncal' + coordinates + ','
        if 'HQprecipitation' in datasets:
            url += 'HQprecipitation' + coordinates + ','
        if 'probabilityLiquidPrecipitation' in datasets:
            url += 'probabilityLiquidPrecipitation' + coordinates + ','
        if 'HQobservationTime' in datasets:
            url += 'HQobservationTime' + coordinates + ','
        if 'randomError' in datasets:
            url += 'randomError' + coordinates + ','
        if 'IRprecipitation' in datasets:
            url += 'IRprecipitation' + coordinates + ','

        url += 'lat[{min_lat}:1:{max_lat}],'.format(min_lat=min_lat, max_lat=max_lat)
        url += 'lon[{min_lon}:1:{max_lon}],'.format(min_lon=min_lon, max_lon=max_lon)
        url += 'time[0:1:0]'

        print(path)
        counter = 5
        while counter > 0:
            try:
                pydap_ds = open_url(url, session=self.session)
                store = xr.backends.PydapDataStore(pydap_ds)
                ds = xr.open_dataset(store)
                if not os.path.isfile(path):
                    ds.to_netcdf(path)
                counter = 0
                return ds
            except Exception as err:
                print(err)
                print('Retrying in 3 seconds...')
                counter = counter - 1
                sleep(3)

    def get_data(self, from_date, to_date, min_lat=None, max_lat=None, min_lon=None, max_lon=None):
        """
        Function to download trmm or gpm data and convert it to an xarray dataset.

        Parameters
        ----------
        dataset_types : str or list of str
            The dataset types variable to be extracted.
        from_date : str
            The start date that you want data in the format 2000-01-01.
        to_date : str
            The end date that you want data in the format 2000-01-01.
        min_lat : int, float, or None
            The minimum lat to extract in WGS84 decimal degrees.
        max_lat : int, float, or None
            The maximum lat to extract in WGS84 decimal degrees.
        min_lon : int, float, or None
            The minimum lon to extract in WGS84 decimal degrees.
        max_lon : int, float, or None
            The maximum lon to extract in WGS84 decimal degrees.

        Returns
        -------
        xarray dataset
            Coordinates are time, lon, lat
        """
        file_path = self.mission_dict['products'][self.product]

        # Must implemented the date verification with min and max dates.
        from_date = pd.Timestamp(from_date)
        to_date = pd.Timestamp(to_date)

        dates = pd.date_range(from_date, to_date)

        base_url = self.mission_dict['base_url']

        # Getting files' url:
        if 'dayofyear' in file_path:
            print("Getting the files' url from NASA server...")
            file_path = os.path.split(file_path)[0]
            iteration = [(date, file_path, base_url) for date in dates]
            url_list = list(itertools.chain.from_iterable(ThreadPool(30).starmap(self.__get_files_urls, iteration)))

        elif 'month' in file_path:
            print('Generating urls...')
            url_list = ['/'.join([base_url, 'opendap', self.mission_dict['process_level'],
                        file_path.format(mission=self.mission.upper(), product=self.product, year=d.year, month=d.month,
                                         date=d.strftime('%Y%m%d'), version=self.version)]) for d in dates]
        return url_list
        # Setting up local urls
        # if 'hyrax' in url_list[0]:
        #     split_text = 'hyrax/'
        # else:
        #     split_text = 'opendap/'
        #
        # url_dict = {url: os.path.join(self.cache_dir, os.path.splitext(url.split(split_text)[1])[0] + '.nc4') for url in
        #             url_list}
        #
        # save_dirs = set([os.path.split(url)[0] for url in url_dict.values()])
        #
        # for path in save_dirs:
        #     if not os.path.exists(path):
        #         os.makedirs(path)
        #
        # # Downloading the data
        # iteration = [(url, path, min_lat, max_lat, min_lon, max_lon) for url, path in url_dict.items()]
        #
        # print('Downloading the data...')
        # output = ThreadPool(30).starmap(self.__download_files, iteration)
        # return output
        # ds_list = []
        # ds_list.extend(output)
        # print('Converting the data...')
        # ds_all = xr.concat(ds_list, dim='time').sortby('time')
        # return ds_all
