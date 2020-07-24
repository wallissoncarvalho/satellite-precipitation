"""
Created on Wed Jul 22 22:27:09 2020
@author: wallissoncarvalho

This file is based on https://github.com/mullenkamp/nasadap
"""
import os
import pandas as pd
import xarray as xr
import requests
from time import sleep
from lxml import etree
import itertools
from multiprocessing.pool import ThreadPool
from pydap.cas.urs import setup_session
from base import mission_product_dict, master_datasets


class Nasa(object):
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
        if product in self.mission_dict['product']:
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

    def __parse_dap_xml(self, date, file_path, process_level, base_url):
        path1 = file_path.format(mission=self.mission.upper(), product=self.product, year=date.year,
                                 dayofyear=date.dayofyear,
                                 version=self.version)
        path2 = '/'.join([process_level, path1])
        url1 = '/'.join([base_url, 'opendap', path2, 'catalog.xml'])
        page1 = requests.get(url1)
        et = etree.fromstring(page1.content)
        urls2 = [base_url + c.attrib['ID'] for c in et.getchildren()[3].getchildren() if not '.xml' in c.attrib['ID']]
        return urls2

    def __download_files(self, url, path, master_dataset_list, dataset_types, min_lat, max_lat, min_lon, max_lon):
        print(path)
        counter = 4
        while counter > 0:
            try:
                store = xr.backends.PydapDataStore.open(url, session=self.session)
                ds = xr.open_dataset(store, decode_cf=False)

                if 'nlon' in ds:
                    ds = ds.rename({'nlon': 'lon', 'nlat': 'lat'})
                ds2 = ds[master_dataset_list].sel(lat=slice(min_lat, max_lat), lon=slice(min_lon, max_lon))

                lat = ds2.lat.values
                lon = ds2.lon.values

                ds_date1 = ds.attrs['FileHeader'].split(';\n')
                ds_date2 = dict([t.split('=') for t in ds_date1 if t != ''])
                ds_date = pd.to_datetime([ds_date2['StopGranuleDateTime']]).tz_convert(None)
                ds2['time'] = ds_date

                for ar in ds2.data_vars:
                    da1 = xr.DataArray(ds2[ar].values.reshape(1, len(lon), len(lat)), coords=[ds_date, lon, lat],
                                       dims=['time', 'lon', 'lat'], name=ar)
                    da1.attrs = ds2[ar].attrs
                    ds2[ar] = da1
                counter = 0

                # Save data as cache
                if not os.path.isfile(path):
                    ds2.to_netcdf(path)
            except Exception as err:
                print(err)
                print('Retrying in 3 seconds...')
                counter = counter - 1
                sleep(3)
        return ds2[dataset_types]

    def __get_data(self, dataset_types, from_date, to_date, min_lat=None, max_lat=None,
                   min_lon=None, max_lon=None, dl_sim_count=30):
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
        dl_sim_count : int
            The number of simultaneous downloads on a single thread. Speed could be increased with more simultaneous
             downloads, but up to a limit of the PC's single thread speed. Also, NASA's opendap server seems to have a
              limit to the total number of simultaneous downloads. 50-60 seems to be around the max.

        Returns
        -------
        xarray dataset
            Coordinates are time, lon, lat
        """
        file_path = self.mission_dict['products'][self.product]
        if isinstance(dataset_types, str):
            dataset_types = [dataset_types]

        # Must implemented the date verification with min and max dates.
        from_date = pd.Timestamp(from_date)
        to_date = pd.Timestamp(to_date)

        dates = pd.date_range(from_date, to_date)

        base_url = self.mission_dict['base_url']

        # Determine files to download:
        if 'dayofyear' in file_path:
            print('Parsing file list from NASA server...')
            file_path = os.path.split(file_path)[0]
            iter2 = [(date, file_path, self.mission, self.mission_dict['process_level'], base_url) for
                     date in dates]
            url_list1 = ThreadPool(30).starmap(self.__parse_dap_xml, iter2)
            url_list = list(itertools.chain.from_iterable(url_list1))

        elif 'month' in file_path:
            print('Generating urls...')
            url_list = ['/'.join([base_url, 'opendap', self.mission_dict['process_level'],
                                  file_path.format(mission=self.mission.upper(), product=self.product, year=d.year,
                                                    month=d.month, date=d.strftime('%Y%m%d'), version=self.version)]) for d
                        in dates]

        if 'hyrax' in url_list[0]:
            split_text = 'hyrax/'
        else:
            split_text = 'opendap/'

        url_dict = {
            u: os.path.join(self.cache_dir, os.path.splitext(u.split(split_text)[1])[0] + '.nc4') for
            u in url_list}

        path_set = set(url_dict.values())
        save_dirs = set([os.path.split(u)[0] for u in url_dict.values()])
        for path in save_dirs:
            if not os.path.exists(path):
                os.makedirs(path)

        # Downloading the data
        iter1 = [(u, u0, self.session, master_datasets[self.product], dataset_types, min_lat, max_lat, min_lon, max_lon) for
                 u, u0 in url_dict.items()]

        output = ThreadPool(dl_sim_count).starmap(self.__download_files, iter1)
        ds_list = []
        ds_list.extend(output)
        ds_all = xr.concat(ds_list, dim='time').sortby('time')
        return ds_all
