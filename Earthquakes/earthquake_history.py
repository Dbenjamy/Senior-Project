import requests
from datetime import datetime
from io import StringIO
from os.path import exists
from os import makedirs
import dask.dataframe as dd
import threading

QUERY_RESPONCE_LIMIT = 20000

class EarthquakeQuery(threading.Thread):
    def __init__(
            self,
            query_ranges,starttime,endtime,
            list_lock) -> None:
        
        super().__init__()
        self.query_ranges = query_ranges
        self.starttime = starttime
        self.endtime = endtime
        self.list_lock = list_lock

    def run(self):
        global QUERY_RESPONCE_LIMIT
        start_date_time = datetime.strptime(self.starttime, "%Y-%m-%d %H:%M:%S")
        end_date_time = datetime.strptime(self.endtime, "%Y-%m-%d %H:%M:%S")
        response = query(start_date_time,end_date_time,query_type='count')

        if response.status_code == 200:
            size = int(response.text)
            if size > QUERY_RESPONCE_LIMIT:
                mid_datetime = start_date_time + (end_date_time - start_date_time) / 2
                mid_date = mid_datetime.strftime("%Y-%m-%d %H:%M:%S")
                
                front = EarthquakeQuery(
                    self.query_ranges,
                    self.starttime,
                    mid_date,
                    self.list_lock)
                back = EarthquakeQuery(
                    self.query_ranges,
                    mid_date,
                    self.endtime,
                    self.list_lock)
                front.start()
                back.start()
                front.join()
                back.join()

            else:
                self.list_lock.acquire()
                self.query_ranges.append(
                    (start_date_time,end_date_time))
                self.list_lock.release()
        else:
            print(f'Failure on query range {start_date_time} - {end_date_time}.',flush=True)

def query(start_date_time,end_date_time,query_type='query'):
    if query_type == 'count':
        url = 'https://earthquake.usgs.gov/fdsnws/event/1/count?'
    elif query_type == 'query':
        url = 'https://earthquake.usgs.gov/fdsnws/event/1/query?'
    params = {
        "format": "csv",
        "starttime": start_date_time,
        "endtime": end_date_time,
        "minmagnitude": "0",
        "orderby": "time-asc",
    }
    response = requests.get(url, params=params)
    return response

def pull_earthquake_data(
        path='./Data',
        starttime='2019-01-01 00:00:00',
        endtime='2022-01-01 00:00:00'):
    data_path = path+'/EarthquakeData/'
    if not exists(data_path):
        makedirs(data_path)

    query_ranges = []
    list_lock = threading.Lock()
    worker = EarthquakeQuery(
        query_ranges,
        starttime=starttime,
        endtime=endtime,
        list_lock=list_lock)
    worker.start()
    worker.join()
    query_ranges = sorted(query_ranges,key=lambda x: x[0])
    
    file_path = data_path + '/earthquake_query_data.csv'
    with open(file_path,'w',errors='ignore') as file:
        head = StringIO(query(*query_ranges[0],'query').text)
        # Write header
        for line in head:
            file.write(line)
            break
        for range in query_ranges:
            text = StringIO(query(*range,'query').text)
            next(text)
            for line in text:
                file.write(line)

def format_earthquake_data(path='./Data'):
    if not exists(path+'/EarthquakeData/EarthquakeEvents'):
        makedirs(path+'/EarthquakeData/EarthquakeEvents')
    
    ddf = dd.read_csv(
        path+'/EarthquakeData/earthquake_query_data.csv',
        encoding='latin-1',
        blocksize='32MB')

    ddf = (
        ddf[ddf['type'] == 'earthquake']
        [['time', 'latitude', 'longitude', 'depth', 'mag', 'magType','rms']]
        .replace('T0[0-5].*','T00:00:00.000',regex=True)
        .replace('T0[6-9].*|T1[0-1].*','T06:00:00.000',regex=True)
        .replace('T1[2-7].*','T12:00:00.000',regex=True)
        .replace('T1[8-9].*|T2[0-3].*','T18:00:00.000',regex=True)
    )
    ddf['time'] = dd.to_datetime(ddf['time'])

    ddf.to_parquet(
        path+'/EarthquakeData/EarthquakeEvents',
        engine="pyarrow",
        compression="snappy",
        name_function=lambda x:f'earthquakes_{x}.parquet')

if __name__ == '__main__':
    pull_earthquake_data()
    format_earthquake_data()
# All column names
#     'time', 'latitude', 'longitude', 'depth', 'mag', 'magType', 'nst',
#     'gap', 'dmin', 'rms', 'net', 'id', 'updated', 'place', 'type',
#     'horizontalError', 'depthError', 'magError', 'magNst', 'status',
#     'locationSource', 'magSource'
