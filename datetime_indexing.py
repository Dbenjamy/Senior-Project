import dask.dataframe as dd
from datetime import datetime

ddf = dd.read_parquet('./Data/EphemData/')#[['CalendarDate(TDB)']]
for date in ddf[['datetime']].head().to_numpy():
    print(date[0].hour)
# print(ddf.head())
# for date in ddf.drop_duplicates().head()['CalendarDate(TDB)']:
#     print(type(date))
#     print(date)


