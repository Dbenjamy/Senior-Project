
import logging
from mpl_toolkits import mplot3d
import matplotlib.pyplot as plt
from graphing_data import create_3d_plot
from planet_data import gravity_scaler
from astropy.constants import R_earth
import pandas as pd
import numpy as np
import glob
import dask.dataframe as dd
from dask import delayed
from os import rename
from dask.distributed import Client
import dask
import dask.bag as db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

G = 6.67430e-11

def magnitude(xyz):
#    return np.sqrt(
#        np.square(xyz_data['gx'])
#        + np.square(xyz_data['gy'])
#        + np.square(xyz_data['gz']))
#    print((xyz[0]**2 + xyz[1]**2 + xyz[2]**2)**0.5)
#    print('##############')
    return (xyz[0]**2 + xyz[1]**2 + xyz[2]**2)**0.5

def gravity(distance,mass):
    result = G*mass/distance**2
    print('Gravity result:')
    print(result)
    print('##############')
    return result

def scale_coord(coord,distance,scaler):
    print('Scaled coords:')
    print([
        coord[0]/distance*scaler,
        coord[1]/distance*scaler,
        coord[2]/distance*scaler])

    print('##############')
    return [
        coord[0]/distance*scaler,
        coord[1]/distance*scaler,
        coord[2]/distance*scaler]

# Function to generate relative coordinates
def relative_coords(coords, planet_data, mass):
#    relative_coords = (-1 * coords + planet_data).astype('float64')
    mag = magnitude(planet_data)
    grav_mag = gravity(mag,mass)
    relative_coords = scale_coord(coords,mag,grav_mag)
#    distance = np.sqrt(np.sum(np.square(relative_coords), axis=1))
#    grav_coords = relative_coords / distance[:, None] * gravity_scaler(relative_coords, mass)[:, None]    grav_coords = 
#    return np.concatenate([planet_data[:, 0].reshape(-1, 1), grav_coords], axis=1)
    print('Relative coords:')
    print([*coords,*relative_coords])
    print('################')
    return [*coords,*relative_coords]


# Function to process each row of h3_data
#def process_row(h3_row, planet_data, mass):
#    coords = np.array([h3_row['X'],h3_row['Y'],h3_row['Z']])
#    rel_coords = relative_coords(coords, planet_data, mass)
#    new_rows = []
#    for entry in rel_coords:
#        grav_mag = np.linalg.norm(entry[1:])
#        new_rows.append([
#            h3_row['geo_code'], pd.to_datetime(entry[0]),
#            h3_row['X'], h3_row['Y'], h3_row['Z'],
#            entry[1], entry[2], entry[3], grav_mag
#        ])
#    return new_rows

# Function to process each partition of h3_data
#@delayed
#def process_partition(partition, planet_data, mass):
#    results = []

#    for _, row in partition.iterrows():
#    for row in partition:
#        results.extend(process_row(row, planet_data, mass))
#    logger.info(f'Processed partition with {len(partition)} rows.')
#    return results
#    return pd.DataFrame(
#        results,
#        columns=[
#            'geo_code', 'datetime', 'X', 'Y', 'Z',
#            'gx', 'gy', 'gz', 'grav_mag'])

#def compute_and_save_object_ephem(data_path, mass):
#    planet_data = dd.read_csv(data_path).compute().to_numpy()
#    h3_data = dd.read_csv('./Data/h3Index/h3_index_0.csv')
#    h3_data['X'] = R_earth * np.sin(h3_data['lat']) * np.cos(h3_data['lon'])
#    h3_data['Y'] = R_earth * np.sin(h3_data['lat']) * np.sin(h3_data['lon'])
#    h3_data['Z'] = R_earth * np.cos(h3_data['lat'])
#    h3_data_list = h3_data.compute().to_dict(orient='records')
#
#    bag = db.from_sequence(h3_data_list,npartitions=24)
#    processed_partitions = bag.map_partitions(process_partition,planet_data,mass)
#    results = processed_partitions.compute()
#    columns = ['geo_code','X','Y','Z','gx','gy','gz','grav_mag']
#    df = pd.DataFrame(results,columns=columns)
#    df.to_csv('./Data/EphemData/testing.csv')
    #    data_generator = [
    #    process_partition(partition, planet_data, mass)
    #    for partition in h3_data.to_delayed()
    #]
    # Convert delayed objects to Dask DataFrame
    #new_data = dd.from_delayed(
    #    data_generator,
    #    meta={
    #        'geo_code': 'O', 'datetime': 'datetime64[ns]', 'X': 'f8', 'Y': 'f8', 'Z': 'f8',
    #        'gx': 'f8', 'gy': 'f8', 'gz': 'f8', 'grav_mag': 'f8'})
    #new_data = new_data.set_index('geo_code')
    #new_data.to_parquet('./Data/EphemData/')

def sum_ephemeris():
    paths = glob.glob('./Data/EphemData/*.parquet')

    final_df = dd.read_parquet(paths[0])
    gravity_dfs = []
    for path in paths[1:]:
        print('Successful start @@@@@@@@')
        gravity_dfs.append(dd.read_parquet(path))
        print('Successful end')

    for df in gravity_dfs:
        final_df = dd.concat(
            [
                final_df[['datetime','X','Y','Z']],
                final_df[['gx','gy','gz']] + df[['gx','gy','gz']]
            ],
            axis=1)

    grav_mag = magnitude(final_df[['gx','gy','gz']]).to_frame('grav_mag')
    final_df = dd.concat([final_df,grav_mag],axis=1)

    print(final_df.head())
    print(final_df['datetime'].max())
    # dd.to_parquet(final_df,'./Data/EphemData/')
    rename(
        './Data/EphemData/part.0.parquet',
        './Data/EphemData/gravity_ephemeris.parquet')

def process_row(h3_data,planet_data):
    coords = np.array([h3_data['X'],h3_data['Y'],h3_data['Z']]).tolist()
    grav_coord = [0.0,0.0,0.0]
    for planet_tuple in planet_data:
        rel_coords = relative_coords(coords, planet_tuple[0], planet_tuple[1])
        grav_coord = [
            grav_coord[0] + rel_coords[0],
            grav_coord[1] + rel_coords[1],
            grav_coord[2] + rel_coords[2]
        ]
#        grav_coord[0] = grav_coord[0] + rel_coords[0]
#        grav_coord[1] = grav_coord[1] + rel_coords[1]
#        grav_coord[2] = grav_coord[2] + rel_coords[2]
    result = planet_data.extend([*grav_coord,magnitude(grav_coord)])
    print('Row results')
    print(result)
    print('###############')
    return result
# np.concatenate([grav_coords, magnitude(grav_coords)], axis=1)

def process_partition(partition,planet_data):
    results = []
    for row in partition:
        results.append(*process_row(row, planet_data))
    logger.info(f'Processed partition with {len(partition)} rows.')
    return results

def generate_file(date,planet_data,h3_data_list):
    
    bag = db.from_sequence(h3_data_list,npartitions=24)
    processed_partitions = bag.map_partitions(process_partition,planet_data)
    results = processed_partitions.compute()
    columns = ['geo_code','X','Y','Z','gx','gy','gz','grav_mag']
    df = pd.DataFrame(results,columns=columns)
    df.to_parquet()

if __name__ == '__main__':
    
#    client = Client(n_workers=24,threads_per_worker=1)
    masses = {
        '10_ephem.csv':1.989e30,
        'Mercury_Barycenter_ephem.csv':3.285e23,
        'Venus_Barycenter_ephem.csv':4.867e24,
        '301_ephem.csv':7.347e22,
        'Mars_Barycenter_ephem.csv': 6.39e23,
        'Jupiter_Barycenter_ephem.csv':1.898e27,
        'Saturn_Barycenter_ephem.csv':5.683e26,
        'Uranus_Barycenter_ephem.csv':8.681e25,
        'Neptune_Barycenter_ephem.csv':1.024e26
    }


    h3_data = dd.read_csv('./Data/h3Index/h3_index_0.csv')
    h3_data['X'] = R_earth * np.sin(h3_data['lat']) * np.cos(h3_data['lon'])
    h3_data['Y'] = R_earth * np.sin(h3_data['lat']) * np.sin(h3_data['lon'])
    h3_data['Z'] = R_earth * np.cos(h3_data['lat'])
    h3_data_list = h3_data.compute().to_dict(orient='records')

#    index = pd.read_csv('./Data/h3_index/h3_index_0.csv')
    ephems = [
        [
            pd.read_csv(f'./Data/EphemData/{file}')[['X','Y','Z']].itertuples(),
            mass
        ]
        for file, mass in masses.items()]
    first_file_name = '10_ephem.csv'
    print(first_file_name)
    datetimes = pd.read_csv(f'./Data/EphemData/{first_file_name}')['CalendarDate(TDB)']

    for date in datetimes.to_list():
        generate_file(
            date,
            [(next(row[0]),row[1]) for row in ephems],
            h3_data_list)
        break

#    for data_path in glob.glob('./Data/EphemData/*.csv'):
#        key = data_path[len('./Data/EphemData/'):]
#        mass = masses[key]
#        logger.info(f'Starting prcessing for {key}')
#        compute_and_save_object_ephem(data_path, mass)
#        logger.info(f'{key[:4]} complete')
#        rename(
#             './Data/EphemData/part.0.parquet',
#             f'./Data/EphemData/{key[:-4]}.parquet')
#        break
#    df = pd.read_csv('./Data/EphemData/10_ephem.csv')
    # print(df.tail())
    # print(len(df))
#    print(df['datetime'].unique())
    
    # sum_ephemeris()
    
    # df = pd.read_parquet('./Data/EphemData/10_ephem.parquet')
    # print(len(df))
    # print(df.head())
    # print(df['datetime'].max())
    # create_3d_plot(df)

    
