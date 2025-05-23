# https://ssd.jpl.nasa.gov/horizons/manual.html
# https://ssd-api.jpl.nasa.gov/doc/horizons_file.html
# https://ssd-api.jpl.nasa.gov/doc/horizons.html#ephem_type

# Codes of major bodies in COMMAND: https://ssd.jpl.nasa.gov/api/horizons.api?format=text&COMMAND=%27MB%27
# Longitude measure East or West of the prime meridian. Positive is eastward
# of the prime meridian
# "https://ssd.jpl.nasa.gov/api/horizons.api?format=text&COMMAND=%27499%27&OBJ_DATA=%27NO%27&CENTER=%27399%27&REF_PLANE=FRAME&VEC_TABLE=%271%27&VEC_CORR=%27LT%2BS%27&EPHEM_TYPE=VECTORS&START_TIME=%272024-01-01%27&STOP_TIME=%272024-01-02%27&STEP_SIZE=%271%20d%27&OUT_UNITS=%27KM-S%27&CSV_FORMAT=YES"
"""
JDTDB: Julian Date, Barycentric Dynamical Time. This is a time standard that is
    used in astronomy and refers to the number of days and fractions of a day
    since January 1, 4713 BC, in the Julian calendar. Barycentric Dynamical
    Time (TDB) is a time standard that accounts for relativistic effects near
    larger celestial bodies.

Calendar Date (TDB): This is the calendar date equivalent of the Julian Date,
    expressed in the more familiar year, month, day format, and it's based on
    Barycentric Dynamical Time.

X, Y, Z: These are the Cartesian coordinates of the planet's position in space,
    relative to a specified reference frame (usually the center of the Earth or
    the solar system's barycenter). They represent the planet's location in
    three-dimensional space.
"""

import requests
import pandas as pd
import numpy as np
from threading import Thread
from threading import Semaphore
from os.path import exists
from os import makedirs
from io import StringIO
import sys

from astropy.time import Time
from astropy.coordinates import ITRS, GCRS, CartesianRepresentation

def eci_to_ecef(eci_coords, time_str):
    t = Time(time_str, scale='utc')
    # Format to ECI coordinates
    eci_rep = CartesianRepresentation(eci_coords)
    # Define GCRS coordinate (equivalent to ECI)
    gcrs = GCRS(eci_rep, obstime=t)
    # Convert GCRS to ITRS (equivalent to ECEF)
    itrs = gcrs.transform_to(ITRS(obstime=t))

    return [time_str, *itrs.cartesian.xyz.value]

def query_ephemeris(object_id,start,end,step):
    
    base_url = "https://ssd.jpl.nasa.gov/api/horizons.api"
    url = (base_url
        + "?format=text"
        + f"&COMMAND='{object_id}'"
        + "&OBJ_DATA='NO'"
        + "&CENTER='500'" # Earth's center
        + "&VEC_TABLE='1'"
        + "&VEC_CORR='LT%2BS'"
        + "&EPHEM_TYPE=VECTORS"
        + "&REF_PLANE=FRAME"
        + f"&START_TIME='{start}'"
        + f"&STOP_TIME='{end}'"
        + f"&STEP_SIZE='{step}'"
        + "&OUT_UNITS='KM-S'"
        + "&CSV_FORMAT=YES")
    response = requests.get(url)
    print(f'Queried data for: {object_id}',flush=True)
    if response.status_code == 200:
        response_text = (response.text
            [response.text.index('JDTDB,'):response.text.index('$$EOE')]
            .replace('\t','')
            .replace(' ',''))
        
        line_start = response_text.index('*')
        return (
            response_text[:line_start]
            + response_text[line_start+129:]
            ).replace(',\n','\n')
    else:
        print(f"Error on {object_id}: {response.status_code}\n",flush=True)

def format_dates(df):
    df['CalendarDate(TDB)'] = (
        pd.to_datetime(( # Trim characters that aren't in iso format
            df['CalendarDate(TDB)'].str[4:15]
            + ' '
            + df['CalendarDate(TDB)'].str[15:17]))
        .dt.strftime('%Y-%m-%d %H:%M:%S.000') # Final formatting
        .astype(str)
    )

def build_object_ephemeris(
        path='./Data',
        object_id='499',
        start='2024-01-01',
        end='2024-01-02',
        step='6 h',
        limit=None):
    if limit == None:
        response_text = query_ephemeris(object_id,start,end,step)
    else:
        limit.acquire()
        response_text = query_ephemeris(object_id,start,end,step)
    
    try:
        df = pd.read_csv(StringIO(response_text))[
            ['CalendarDate(TDB)','X','Y','Z']]
    except pd.errors.EmptyDataError:
        if limit != None: limit.release()
        return

    format_dates(df)
    ephem_array = np.array(
        dtype='object',
        object=list(map(
            lambda row: eci_to_ecef(eci_coords=row[1:],time_str=row[0]),
            df.to_numpy())))

    # Saving
    file_path = path+'/EphemData/'+object_id.replace(' ','_')+'_ephem.csv'
    pd.DataFrame(
        ephem_array,columns=['CalendarDate(TDB)','X','Y','Z']
        ).to_csv(file_path,index=False)
    if limit != None: limit.release()

def build_planets_ephems(
        path='./Data',
        object_ids:list = ['301'],
        start='2019-01-01',
        end='2019-01-05',
        step='6 h'):
    if not exists(path+'/EphemData'):
        makedirs(path+'/EphemData')
    
    workers = []
    limit = Semaphore(2) # Seems that only two queries allowed at once
    for obj_id in object_ids:
        kwargs = {
            'object_id':obj_id,
            'start':start,
            'end':end,
            'step':step,
            'path':path,
            'limit':limit
        }
        workers.append(Thread(target=build_object_ephemeris,kwargs=kwargs))
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()

if __name__ == '__main__':
    objects_and_masses = {
        '10':1.989e30,
        'Mercury Barycenter':3.285e23,
        'Venus Barycenter':4.867e24,
        '301':7.347e22, # Earth's moon
        'Mars Barycenter': 6.39e23,
        'Jupiter Barycenter':1.898e27,
        'Saturn Barycenter':5.683e26,
        'Uranus Barycenter':8.681e25,
        'Neptune Barycenter':1.024e26
    }
    build_planets_ephems(object_ids=objects_and_masses.keys())

