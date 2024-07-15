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
from io import StringIO
try:
    from Ephemeris.convert_coordinates import eci_to_ecef
except ModuleNotFoundError:
    from convert_coordinates import eci_to_ecef

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
    print(response.url)
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
        print(f"Error: {response.status_code}")

def format_dates(df):
     df['CalendarDate(TDB)'] = (
        pd.to_datetime(( # Trim characters that aren't in iso format
            df['CalendarDate(TDB)'].str[4:15]
            + ' '
            + df['CalendarDate(TDB)'].str[15:17]))
        .dt.strftime('%Y-%m-%d %H:%M:%S.000') # Final formatting
        .astype(str))

def object_ephemeris(
        object_id='499',
        start='2024-01-01',
        end='2024-01-02',
        step='1 h',
        path=None):
    
    response_text = query_ephemeris(object_id,start,end,step)
    df = pd.read_csv(StringIO(response_text))[
        ['CalendarDate(TDB)','X','Y','Z']]
    format_dates(df)
    
    ephem_array = np.array(dtype='object',object=list(map(
        lambda row: eci_to_ecef(eci_coords=row[1:],time_str=row[0]),
        df.to_numpy())))
    
    if path == None:
        file_path = f"{object_id}_ephemerides.csv"
    else:
        file_path = path
    pd.DataFrame(
        ephem_array,columns=['CalendarDate(TDB)','X','Y','Z']
        ).to_csv(file_path,index=False)

if __name__ == '__main__':

    planets = [
        '10',
        'Mercury Barycenter',
        'Venus Barycenter',
        '301', # Earth's moon
        'Mars Barycenter',
        'Jupiter Barycenter',
        'Saturn Barycenter',
        'Uranus Barycenter',
        'Neptune Barycenter'
    ]
    for object in [planets[0]]:
        
        object_ephemeris(
            object_id=object,
            start='2019-01-01',
            end='2022-01-01',
            step='6 h',
            path=f'.\\Data\\EphemData\\{object.replace(' ','_')}_ephem.csv'
        )

