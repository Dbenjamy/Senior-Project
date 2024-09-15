import csv
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import multiprocessing as mp
from time import sleep
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

def gravity_from_date(work_queue,write_queue):
    while True:
        results = []
        task = work_queue.get()
        if task == None:
            work_queue.put(None)
            break

        num, date, h3_data, ephems = task
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
        write_queue.put((num,np.asarray(results)))

def work_scheduler(path,dates,masses,work_queue,write_queue):
    h3_data = np.column_stack(
        [col.to_numpy() for col in
            pq.read_table(path+'/h3Index/').columns])
    ephem_path = path+'/EphemData/{}_ephem.csv'
    ephems = []
    for obj_id, mass in masses.items():
        planet_path = ephem_path.format(obj_id.replace(' ','_'))
        planet_array = csv_to_numpy(planet_path)
        ephems.append((planet_array,mass))
    
    for i, date in enumerate(dates):
        filtered = []
        for planet, mass in ephems:
            planet_filtered = planet[planet[:,0]==date][:,1:][0]
            filtered.append((planet_filtered,mass))
        work_queue.put((i,date,h3_data,filtered))
    while True:
        if work_queue.empty() and write_queue.empty():
            work_queue.put(None)
            break
        sleep(1)

def write_parquet(path,array:np.ndarray):
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
    pq.write_table(table,path)

def save_partitions(path,write_queue,chunk_size:int):
    order_num, start_array = write_queue.get()
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
        task = write_queue.get()
        if task == None:
            break
        order_num, array = task
        completed_dict[order_num] = array
        save_checks(stats,completed_dict,array_list)
    while not completed_dict:
        save_checks(stats,completed_dict)
    
    if len(array_list) > 0:
        full_array = np.concatenate(array_list,axis=0)
        write_parquet(save_path.format(stats['part_num']),full_array)

def save_checks(stats,completed_dict,array_list):
    if stats['part_num'] in completed_dict:
        next_array = completed_dict[stats['part_num']]
        if stats['local_total'] + len(next_array) > stats['chunk_size']:
            full_array = np.concat(array_list,axis=0)
            write_parquet(stats['path'].format(stats['part_num']),full_array)
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

    work_queue = mp.Manager().Queue()
    write_queue = mp.Manager().Queue()
    workers = []
    for _ in range(mp.cpu_count()-1):
        workers.append(
            mp.Process(
                target=gravity_from_date,
                args=(work_queue,write_queue))
        )
    director = mp.Process(target=work_scheduler,args=(path,datetimes,masses,work_queue,write_queue))
    writer = mp.Process(target=save_partitions,args=(path,write_queue,30e6))
    for worker in workers: worker.start()
    writer.start()
    director.start()
    
    for worker in workers: worker.join()
    director.join()
    write_queue.put(None)
    writer.join()


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
    import dask.dataframe as dd
    print(dd.read_parquet('./Data/GravityData/'))

    
    # create_3d_plot(df)

    
