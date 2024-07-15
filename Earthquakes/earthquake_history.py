import requests
from datetime import datetime
from io import StringIO

QUERY_RESPONCE_LIMIT = 20000

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
    # response.encoding = response.apparent_encoding
    return response

def get_ranges(starttime, endtime, valid_ranges=None):
    if valid_ranges == None:
        valid_ranges = []
    
    start_date_time = datetime.strptime(starttime, "%Y-%m-%d %H:%M:%S")
    end_date_time = datetime.strptime(endtime, "%Y-%m-%d %H:%M:%S")

    response = query(start_date_time,end_date_time,query_type='count')

    if response.status_code == 200:
        size = int(response.text)
        if size > QUERY_RESPONCE_LIMIT:
            mid_datetime = start_date_time + (end_date_time - start_date_time) / 2
            mid_date = mid_datetime.strftime("%Y-%m-%d %H:%M:%S")
            
            get_ranges(starttime, mid_date, valid_ranges)
            get_ranges(mid_date, endtime, valid_ranges)
        else:
            valid_ranges.append((start_date_time,end_date_time))
    else:
        print('failure')

def get_data(starttime,endtime):

    start_date_time = datetime.strptime(starttime, "%Y-%m-%d %H:%M:%S")
    end_date_time = datetime.strptime(endtime, "%Y-%m-%d %H:%M:%S")
    
    return query(start_date_time,end_date_time,query_type='query')


if __name__ == '__main__':
    query_ranges = []
    get_ranges(
        '2019-01-01 00:00:00',
        '2022-01-01 00:00:00',
        query_ranges)
    
    # Write header
    file_path = f'.\\Data\\EarthquakeData\\earthquake_test.csv'
    with open(file_path,'w',errors='ignore') as file:
        head = StringIO(query(*query_ranges[0],'query').text)
        for line in head:
            file.write(line)
            break
        for range in query_ranges:

            text = StringIO(query(*range,'query').text)
            next(text)
            for line in text:
                file.write(line)
