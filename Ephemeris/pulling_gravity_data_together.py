
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
import dask.multiprocessing

G = 6.67430e-11

def magnitude(xyz):
    return (xyz[0]**2 + xyz[1]**2 + xyz[2]**2)**0.5

def gravity(distance,mass):
    return G*mass/distance**2

def scale_coord(coord,distance,scaler):
    return [
        coord[0]/distance*scaler,
        coord[1]/distance*scaler,
        coord[2]/distance*scaler]

def sum_ephemeris(path):
    paths = glob.glob(path+'/EphemData/*.parquet')

    final_df = dd.read_parquet(paths[0])
    gravity_dfs = []
    for path in paths[1:]:
        gravity_dfs.append(dd.read_parquet(path))

    for df in gravity_dfs:
        final_df = dd.concat(
            [
                final_df[['datetime','X','Y','Z']],
                final_df[['gx','gy','gz']] + df[['gx','gy','gz']]
            ],
            axis=1)

    grav_mag = magnitude(final_df[['gx','gy','gz']]).to_frame('grav_mag')
    final_df = dd.concat([final_df,grav_mag],axis=1)
    rename(
        path+'/EphemData/part.0.parquet',
        path+'/EphemData/gravity_ephemeris.parquet')


def relative_coords(coords, planet_tuple, mass):
    planet_data = [planet_tuple[1],planet_tuple[2],planet_tuple[3]]
    mag = magnitude(planet_data)
    grav_mag = gravity(mag,mass)
    relative_coords = scale_coord(coords,mag,grav_mag)
    return relative_coords

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
    result = [*grav_coord,magnitude(grav_coord)]
    return result

def process_partition(partition,date,planet_data):
    results = []
    for row in partition:
        results.append([
            row['geo_code'],
            str(date),
            row['X'],
            row['Y'],
            row['Z'],
            *process_row(row, planet_data)
        ])
    return results

def generate_file(path,num,date,planet_data,h3_data_list):
    bag = db.from_sequence(h3_data_list,npartitions=24)
    processed_partitions = bag.map_partitions(process_partition,date,planet_data)

    columns = ['geo_code','date','X','Y','Z','gx','gy','gz','grav_mag']
    results = (
        processed_partitions
        .to_dataframe(columns=columns)
        .compute()
        .set_index('date'))
    data_path = path+f'/EphemData/grav_ephem_{num}.parquet'
    results.to_parquet(data_path,engine='pyarrow')

def build_gravity_dataset(path,masses):
    client = Client(n_workers=12,threads_per_worker=2)
    
    h3_data = dd.read_csv(path+'/h3Index/h3_index_0.csv')
    h3_data['X'] = R_earth * np.sin(h3_data['lat']) * np.cos(h3_data['lon'])
    h3_data['Y'] = R_earth * np.sin(h3_data['lat']) * np.sin(h3_data['lon'])
    h3_data['Z'] = R_earth * np.cos(h3_data['lat'])
    h3_data_list = h3_data.compute().to_dict(orient='records')

    ephems = [
        [
            pd.read_csv(
                    path
                    + '/EphemData/'
                    + obj_id.replace(' ','_')
                    + '_ephem.csv')
                [['X','Y','Z']]
                .itertuples(),
            mass
        ] for obj_id, mass in masses.items()]
    # Getting dates from file to iterate later
    file_name = list(masses.keys())[0].replace(' ','_') + '_ephem.csv'
    datetimes = (
        pd.read_csv(
            path + f'/EphemData/{file_name}')
        ['CalendarDate(TDB)']
        .to_list())
    
    count = 1
    for date in datetimes:
        generate_file(
            path,
            count,
            date,
            [(next(row[0]),row[1]) for row in ephems],
            h3_data_list)
        print(f'Completed {count}/{len(datetimes)}')
        count += 1


if __name__ == '__main__':
    path = './Data'
    masses = {
        '10':1.989e30,
        'Mercury Barycenter':3.285e23,
        'Venus Barycenter':4.867e24,
        '301':7.347e22,
        'Mars Barycenter': 6.39e23,
        'Jupiter Barycenter':1.898e27,
        'Saturn Barycenter':5.683e26,
        'Uranus Barycenter':8.681e25,
        'Neptune Barycenter':1.024e26
    }
    build_gravity_dataset(path=path,masses=masses)
    
    # create_3d_plot(df)

    
