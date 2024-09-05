import requests
from datetime import datetime
from io import StringIO
import dask.dataframe as dd
import threading

QUERY_RESPONCE_LIMIT = 20000

class Query(threading.Thread):
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
                
                front = Query(
                    self.query_ranges,
                    self.starttime,
                    mid_date,
                    self.list_lock)
                back = Query(
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
                self.query_ranges.append((start_date_time,end_date_time))
                self.list_lock.release()
        else:
            print(f'Failure on query range {start_date_time} - {end_date_time}.')

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

if __name__ == '__main__':
    data_path = './Data/EarthquakeData/'
    query_ranges = []
    list_lock = threading.Lock()
    worker = Query(
        query_ranges,
        starttime='2019-01-01 00:00:00',
        endtime='2022-01-01 00:00:00',
        list_lock=list_lock)
    worker.start()
    worker.join()
    
    query_ranges = sorted(query_ranges,key=lambda x: x[0])
    file_path = data_path + 'earthquake_test.csv'
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
    # Convert CSV to Parquet
    df = dd.read_csv(path=data_path,encoding='latin-1', blocksize="32MB")
    df.to_parquet(data_path, engine="pyarrow", compression="snappy")
