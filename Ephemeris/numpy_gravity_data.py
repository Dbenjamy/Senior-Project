
import csv
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import multiprocessing as mp
from os.path import exists
from os import makedirs

G = 6.67430e-11

def magnitude(xyz):
    return (xyz[0]**2 + xyz[1]**2 + xyz[2]**2)**0.5

def gravity(distance,mass):
    return G*mass/distance**2

def scale_coord(coord,distance,scaler):
    return [
        coord[0]/distance*scaler,
        coord[1]/distance*scaler,
        coord[2]/distance*scaler]

def relative_coords(coords, planet, mass):
    mag = magnitude(planet)
    grav_mag = gravity(mag,mass)
    relative_coords = scale_coord(coords,mag,grav_mag)
    return relative_coords

def process_row(h3_coord,ephems):
    grav_coord = [0.0,0.0,0.0]
    
    for planet, mass in ephems:
        rel_coords = relative_coords(h3_coord, planet, mass)
        grav_coord = [
            grav_coord[0] + rel_coords[0],
            grav_coord[1] + rel_coords[1],
            grav_coord[2] + rel_coords[2]
        ]
    result = [*grav_coord,magnitude(grav_coord)]
    return result

def process_partition(partition,date,planet_data):
    results = []
    for row in partition:
        results.append([
            row['geo_code'],
            str(date),
            row['X'],
            row['Y'],
            row['Z'],
            *process_row(row, planet_data)
        ])
    return results

def gravity_from_date(date,request_queue):
    results = []
    data_send, data_recieve = mp.Pipe()
    request_queue.put((date,data_send))
    h3_data, ephems = data_recieve.recv()
    for row in h3_data:
        results.append([
            row[0],
            str(date),
            row[1],
            row[2],
            row[3],
            *process_row(row[1:],ephems)
        ])
    # columns = ['geo_code','date','X','Y','Z','gx','gy','gz','grav_mag']
    return np.asarray(results)


def big_data_scheduler(iterator,request_queue,completed_queue):
    schedule = []
    schedule.append((iterator[0],request_queue,completed_queue,0))
    order_num = 1
    for item in iterator[0:]:
        schedule.append((item,request_queue,completed_queue,order_num))
        order_num += 1
    return schedule

def data_transmitter(path,masses,request_queue):
    h3_data = np.column_stack(
        [col.to_numpy() for col in
            pq.read_table(path+'/h3Index/').columns])
    ephem_path = path+'/EphemData/{}_ephem.csv'
    ephems = []
    for obj_id, mass in masses.items():
        planet_path = ephem_path.format(obj_id.replace(' ','_'))
        planet_array = csv_to_numpy(planet_path)
        ephems.append((planet_array,mass))
    while True:
        date, response = request_queue.get()
        filtered = []
        for planet, mass in ephems:
            planet_filtered = planet[planet[:,0]==date][:,1:][0]
            filtered.append((planet_filtered,mass))
        print(filtered[0][0])
        response.send((h3_data,filtered))

def worker(task):
    date, request_queue, completed_queue, order_num = task
    array = gravity_from_date(date, request_queue)
    completed_queue.put((order_num,array))
    #
    # Get rid of worker and change gravity
    # 

def write_table(path,array:np.ndarray):
    table = pa.table({
        'geo_code':array[:,0],
        'date':array[:,1],
        'X':array[:,2],
        'Y':array[:,3],
        'Z':array[:,4],
        'gx':array[:,5],
        'gy':array[:,6],
        'gz':array[:,7],
        'grav_mag':array[:,8]
    })
    pa.write_table(table,path)

def save_partitions(path,completed_queue,chunk_size:int):
    order_num, start_array = completed_queue.get()
    assert type(start_array) == np.ndarray
    array_list = [np.copy(start_array)]

    data_path = path+'/GravityData/'
    if not exists(data_path):
        makedirs(data_path)
    save_path = data_path+'gravity_{}.parquet'

    stats = {
        'local_total':len(start_array),
        'part_num':1,
        'chunk_size':chunk_size,
        'path':save_path,
    }
    del start_array
    completed_dict = dict()

    while True:
        order_num, array = completed_queue.get()
        if type(array) != np.ndarray and array == 'stop':
            break
        completed_dict[order_num] = array
        save_checks(stats,completed_dict,array_list)

    while not completed_dict:
        save_checks(stats,completed_dict)
    
    if len(array_list) > 0:
        full_array = np.concat(array_list,axis=0)
        write_table(save_path.format(stats['part_num']),full_array)

def save_checks(stats,completed_dict,array_list):
    if stats['part_num'] in completed_dict:
        next_array = completed_dict[stats['part_num']]
        if stats['local_total'] + len(next_array) > stats['chunk_size']:
            full_array = np.concat(array_list,axis=0)
            write_table(stats['path'].format(stats['part_num']),full_array)
            print(f'Completed {stats['part_num']}')
            stats['local_total'] = len(next_array)
            array_list = [next_array]
            del completed_dict[stats['part_num']]
            stats['part_num'] += 1
        else:
            stats['local_total'] += len(next_array)
            array_list.append(next_array)
            del completed_dict[stats['part_num']]

def csv_to_numpy(full_path):
    with open(full_path,'r') as file:
        data_gen = csv.reader(file,delimiter=',')
        next(data_gen)
        data = [[
                datetime.strptime(row[0][:-4],'%Y-%m-%d %H:%M:%S'),
                float(row[1]),
                float(row[2]),
                float(row[3])]
            for row in data_gen]
        
        return np.asarray(data)


def build_gravity_dataset(path,masses):
    # Getting dates from file to iterate later
    ephem_path = path+'/EphemData/{}_ephem.csv'
    datefile_name = ephem_path.format(list(masses.keys())[0].replace(' ','_'))

    with open(datefile_name,'r') as file:
        data_gen = csv.reader(file,delimiter=',')
        next(data_gen)
        datetimes = np.asarray(
            [datetime.strptime(row[0][:-4],'%Y-%m-%d %H:%M:%S')
            for row in data_gen])

    request_queue = mp.Manager().Queue()
    completed_queue = mp.Manager().Queue()
    schedule = [task for task in big_data_scheduler(datetimes,request_queue,completed_queue)]
    data_process = mp.Process(target=data_transmitter,args=(path,masses,request_queue))
    main = mp.Process(target=save_partitions,args=(path,completed_queue,30e6))
    data_process.start()
    main.start()
    with mp.Pool(mp.cpu_count()) as pool:
        pool.map(worker,schedule)


if __name__ == '__main__':
    path = './Data'
    masses = {
        '10':1.989e30,
        'Mercury Barycenter':3.285e23,
        'Venus Barycenter':4.867e24,
        '301':7.347e22,
        'Mars Barycenter': 6.39e23,
        'Jupiter Barycenter':1.898e27,
        'Saturn Barycenter':5.683e26,
        'Uranus Barycenter':8.681e25,
        'Neptune Barycenter':1.024e26
    }
    build_gravity_dataset(path=path,masses=masses)
    
    # create_3d_plot(df)

    
