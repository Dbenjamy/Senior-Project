import dask.dataframe as dd
import glob

# for file in glob.glob('./Data/EphemData/*.csv'):
#     print(file)
#     print(dd.read_csv(file).head())
#     break


ddf = dd.read_parquet('./Data/EarthquakeData/EarthquakeEvents/')
print(ddf.head())
print(ddf['time'].value_counts().compute())
print(ddf[['time']].head(10))
print(ddf[['time']].tail(10))
