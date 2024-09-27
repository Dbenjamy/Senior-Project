from Earthquakes.earthquake_history import pull_earthquake_data, format_earthquake_data
from DataStructure.h3_data_structure import build_h3_index
from Ephemeris.ephem_requests import build_planets_ephems
from Ephemeris.numpy_gravity_data import build_gravity_dataset
from Ephemeris.graphing_data import create_3d_plot
import pandas as pd

if __name__ == '__main__':
    DATA_PATH = './Data'
    START = '2019-01-01 00:00:00'
    END = '2019-01-05 00:00:00'
    RESOLUTION = 2

    objects_and_masses = {
        '10':1.989e30, # Sun
        'Mercury Barycenter':3.285e23,
        'Venus Barycenter':4.867e24,
        '301':7.347e22, # Earth's moon
        'Mars Barycenter': 6.39e23,
        'Jupiter Barycenter':1.898e27,
        'Saturn Barycenter':5.683e26,
        'Uranus Barycenter':8.681e25,
        'Neptune Barycenter':1.024e26
    }
    pull_earthquake_data(path=DATA_PATH,starttime=START,endtime=END)
    format_earthquake_data(path=DATA_PATH)
    build_h3_index(path=DATA_PATH,resolution=RESOLUTION,output_prefix='h3_index')
    build_planets_ephems(
        path=DATA_PATH,
        object_ids=objects_and_masses.keys(),
        start=START,
        end=END)
    build_gravity_dataset(path=DATA_PATH,masses=objects_and_masses)

    # This demonstation graph only works with one file. The entire dataset is
    # loaded into memory, so only small datasets are recommended
    ddf = pd.read_parquet(DATA_PATH+'/GravityData/gravity_0.parquet')
    create_3d_plot(ddf)
