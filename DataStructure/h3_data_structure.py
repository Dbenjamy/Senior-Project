import h3
from os.path import exists
from os import makedirs
import numpy as np
import pandas as pd
from astropy.constants import R_earth

def h3_index_generator(parents, resolution=1):
    for parent in parents:
        for item in h3.h3_to_children(parent, resolution):
            index = str(item)
            lat, lon = h3.h3_to_geo(item)
            yield (index,lat,lon)

def build_h3_index(
        path='./Data',
        resolution=3,
        chunk_size=2000000,
        output_prefix='index'):

    if not exists(path+'/h3Index'):
        makedirs(path+'/h3Index')

    parents = h3.get_res0_indexes()
    gen = h3_index_generator(parents,resolution=resolution)
    counter = 0
    while True:
        data = pd.DataFrame.from_records(
            gen,
            columns=['geo_code','lat','lon'],
            nrows=chunk_size)
        data['X'] = R_earth * np.sin( data['lat']) * np.cos( data['lon'])
        data['Y'] = R_earth * np.sin( data['lat']) * np.sin( data['lon'])
        data['Z'] = R_earth * np.cos( data['lat'])
        file_save_path = f'{path}/h3Index/{output_prefix}_{counter}.parquet'
        data[['geo_code','X','Y','Z']].to_parquet(file_save_path)
        if len(data) < chunk_size:
            break
        counter += 1

if __name__ == '__main__':
    build_h3_index(resolution=2,output_prefix='h3_index')