
import dask.dataframe as dd
from astropy.constants import R_earth
import numpy as np

def magnitude(xyz_dataframe):
    return np.sqrt(
        np.square(xyz_dataframe['X'])
        + np.square(xyz_dataframe['Y'])
        + np.square(xyz_dataframe['Z'])
        )

# On a small sample (less than 217), the shortest distance between 
# any two points is 17.69 km. There are 531484 earthquake observations. For
# now I'll use resolution 4 in h3, but 5 seems more appropriate.
def calculate_distance():
    R_km = R_earth/1000
    ddf = dd.read_parquet('./Data/EarthquakeData/',columns=['latitude','longitude','depth'])
    ddf['X'] = (R_km - ddf['depth']) * np.sin(ddf['latitude']) * np.cos(ddf['longitude'])
    ddf['Y'] = (R_km - ddf['depth']) * np.sin(ddf['latitude']) * np.sin(ddf['longitude'])
    ddf['Z'] = (R_km - ddf['depth']) * np.cos(ddf['latitude'])
    
    coord_ddf = ddf[['X','Y','Z']].sample(frac=1/164/15)

    increment = 100/len(coord_ddf)
    counter = 0.0
    min_dist = float('inf')
    try:
        for row in coord_ddf.itertuples():
            # start = time()
            distances = magnitude(coord_ddf - row[1:]).compute()
            try:
                local_min = np.min(distances[distances > 0.0])
                if local_min < min_dist:
                    min_dist = local_min
            except ValueError:
                pass
            finally:
                counter += increment
                if counter.is_integer():
                    print(f'%{round(counter)}')
                continue
    except ValueError:
        pass

    print(min_dist)
if __name__ == '__main__':
    calculate_distance()


