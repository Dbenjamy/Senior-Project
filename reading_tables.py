import dask.dataframe as dd
import glob

for file in glob.glob('./Data/EphemData/*.csv'):
    print(file)
    print(dd.read_csv(file).head())
    break


# ddf = dd.read_parquet('./Data/EarthquakeData/')

# print(ddf[['time']].head(20))
