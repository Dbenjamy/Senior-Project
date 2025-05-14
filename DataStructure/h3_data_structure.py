import h3
from os.path import exists
from os import makedirs
import numpy as np
import pandas as pd
from astropy.constants import R_earth
from typing import Generator

def h3_index_generator(parents:set[str], resolution=1):
    for parent in parents:
        for item in h3.h3_to_children(parent, resolution):
            index = str(item)
            lat, lon = h3.h3_to_geo(item)
            yield index, lat , lon

def build_h3_index(
        path='./Data',
        resolution=2,
        chunk_size=800000,
        output_prefix='index'):
    if not exists(path+'/h3Index'):
        makedirs(path+'/h3Index')
    
    counter = 0
    parents:set[str] = h3.get_res0_indexes()
    gen:Generator[tuple[str,float,float]]|tuple[tuple[str,float,float]]
    file_save_path:str
    if resolution < 5:
        gen = tuple(h3_index_generator(parents,resolution=resolution))
        file_save_path = f'{path}/h3Index/{output_prefix}_out.parquet'
    else:
        gen = h3_index_generator(parents,resolution)
        file_save_path = f'{path}/h3Index/{output_prefix}_out_{counter}.parquet'
    while True:
        data = pd.DataFrame.from_records(
            gen,columns=['geo_code','lat','lon'],nrows=chunk_size)
        data['X'] = R_earth * np.sin( data['lat']) * np.cos( data['lon'])
        data['Y'] = R_earth * np.sin( data['lat']) * np.sin( data['lon'])
        data['Z'] = R_earth * np.cos( data['lat'])
        data['geo_code'] = data['geo_code'].str.slice(stop=code_trucate_limit(resolution))
        data.sort_values(by='geo_code',inplace=True)
        data[['geo_code','X','Y','Z']].to_parquet(file_save_path,index=False)
        if len(data) < chunk_size:
            break
        counter += 1

def code_trucate_limit(resolution:int):
    return (5   if resolution == 1
        else 6  if resolution < 4
        else 7  if resolution == 4
        else 8  if resolution == 5
        else 9  if resolution < 8
        else 10 if resolution == 8
        else 11 if resolution == 9
        else 12 if resolution < 12
        else 13 if resolution == 12
        else 14 if resolution == 13
        else 15 if resolution == 14
        else 16)

if __name__ == '__main__':
    build_h3_index(resolution=2,output_prefix='h3_index')