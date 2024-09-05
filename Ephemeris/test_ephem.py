from graphing_data import create_3d_plot
from planet_data import gravity_scaler
from astropy.constants import R_earth, G
import pandas as pd
import numpy as np
import glob
import dask.dataframe as dd
from dask import delayed
from os import rename
import dask.bag as db
import dask.multiprocessing

def scale_coord(coord,distance,scaler):
    return [
        coord[0]/distance*scaler,
        coord[1]/distance*scaler,
        coord[2]/distance*scaler]

def magnitude(xyz):
    return (xyz[0]**2 + xyz[1]**2 + xyz[2]**2)**0.5

def gravity(distance,mass):
    return G*mass/distance**2

def relative_coords(coords, planet_tuple, mass):
    planet_data = [planet_tuple[1],planet_tuple[2],planet_tuple[3]]
    mag = magnitude(planet_data)
    grav_mag = gravity(mag,mass)
    relative_coords = scale_coord(coords,mag,grav_mag)
    return relative_coords


# Function to process each row of h3_data
def process_row(h3_row, planet_data, mass):
   coords = np.array([h3_row['X'],h3_row['Y'],h3_row['Z']])
   rel_coords = relative_coords(coords, planet_data, mass)
   new_rows = []
   for entry in rel_coords:
       grav_mag = np.linalg.norm(entry[1:])
       new_rows.append([
           h3_row['geo_code'], pd.to_datetime(entry[0]),
           h3_row['X'], h3_row['Y'], h3_row['Z'],
           entry[1], entry[2], entry[3], grav_mag
       ])
   return new_rows

# Function to process each partition of h3_data
@delayed
def process_partition(partition, planet_data, mass):
    results = []

    for _, row in partition.iterrows():
        results.extend(process_row(row, planet_data, mass))
    return pd.DataFrame(
       results,
       columns=[
           'geo_code', 'datetime', 'X', 'Y', 'Z',
           'gx', 'gy', 'gz', 'grav_mag'])

def compute_and_save_object_ephem(data_path, mass):
    planet_data = dd.read_csv(data_path).compute().to_numpy()
    h3_data = dd.read_csv('./Data/h3Index/h3_index_0.csv')
    h3_data['X'] = R_earth * np.sin(h3_data['lat']) * np.cos(h3_data['lon'])
    h3_data['Y'] = R_earth * np.sin(h3_data['lat']) * np.sin(h3_data['lon'])
    h3_data['Z'] = R_earth * np.cos(h3_data['lat'])
    h3_data_list = h3_data.compute().to_dict(orient='records')  
    bag = db.from_sequence(h3_data_list,npartitions=24)
    processed_partitions = bag.map_partitions(process_partition,planet_data,mass)
    results = processed_partitions.compute()
    columns = ['geo_code','X','Y','Z','gx','gy','gz','grav_mag']
    df = pd.DataFrame(results,columns=columns)
    df.to_csv('./Data/EphemData/testing.csv')
    data_generator = [
        process_partition(partition, planet_data, mass)
        for partition in h3_data.to_delayed()
    ]
    # Convert delayed objects to Dask DataFrame
    new_data = dd.from_delayed(
       data_generator,
       meta={
           'geo_code': 'O', 'datetime': 'datetime64[ns]', 'X': 'f8', 'Y': 'f8', 'Z': 'f8',
           'gx': 'f8', 'gy': 'f8', 'gz': 'f8', 'grav_mag': 'f8'})
    new_data = new_data.set_index('geo_code')
    new_data.to_parquet('./Data/EphemData/')
