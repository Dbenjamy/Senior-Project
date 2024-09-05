import dask.dataframe as dd

ddf = dd.read_parquet('./Data/EarthquakeData/CSVtoParquet')

ddf['time'] = dd.to_datetime(ddf['time'])
ddf = ddf.sort_values('time')
ddf['time'] = ddf['time'].astype(str)

ddf = (
    ddf[ddf['type'] == 'earthquake']
    [['time', 'latitude', 'longitude', 'depth', 'mag', 'magType','rms']]
    .replace(' 0[0-9].*| 1[0-2].*',' 00:00:00.000',regex=True)
    .replace(' 1[3-9].*| 2[0-4].*',' 12:00:00.000',regex=True)    
)

ddf['time'] = dd.to_datetime(ddf['time'])

ddf.to_parquet(
    './Data/EarthquakeData/EarthquakeEvents',
    engine="pyarrow",
    compression="snappy",
    name_function=lambda x:f'earthquakes_{x}.parquet')

# All column names
#     'time', 'latitude', 'longitude', 'depth', 'mag', 'magType', 'nst',
#     'gap', 'dmin', 'rms', 'net', 'id', 'updated', 'place', 'type',
#     'horizontalError', 'depthError', 'magError', 'magNst', 'status',
#     'locationSource', 'magSource'
