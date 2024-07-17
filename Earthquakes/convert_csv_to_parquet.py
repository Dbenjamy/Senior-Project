
import dask.dataframe as dd
import pyarrow as pa

input_file = "./Data/EphemData/10_ephem.csv"
output_directory = "./Data/EphemData/"

df = dd.read_csv(path=input_file,encoding='latin-1', blocksize="32MB")

# Convert Dask DataFrame to Parquet
df.to_parquet(output_directory, engine="pyarrow", compression="snappy")
