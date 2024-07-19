
import dask.dataframe as dd


read =  './Data/EphemData/EphemParquet/'
write = './Data/EphemData/EphemRepartitioned/'

df = dd.read_parquet(read)
df.repartition(npartitions=200).to_parquet(write)