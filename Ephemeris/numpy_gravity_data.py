import csv
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import multiprocessing as mp
from time import sleep
from os.path import exists
from os import makedirs

class Director(mp.Process):
    def __init__(self,path,datetimes,masses,work_queue,write_queue):
        super().__init__()
        self.path = path
        self.datetimes = datetimes
        self.masses = masses
        self.work_queue = work_queue
        self.write_queue = write_queue

    def run(self):
        h3_data = np.column_stack(
            [col.to_numpy() for col in
                pq.read_table(self.path+'/h3Index/').columns])
        ephem_path = self.path+'/EphemData/{}_ephem.csv'
        ephems = []
        for obj_id, mass in self.masses.items():
            planet_path = ephem_path.format(obj_id.replace(' ','_'))
            planet_array = self.csv_to_numpy(planet_path)
            ephems.append((planet_array,mass))
        for i, date in enumerate(self.datetimes):
            filtered = []
            for planet, mass in ephems:
                planet_filtered = planet[planet[:,0]==date][:,1:][0]
                filtered.append((planet_filtered,mass))
            self.work_queue.put((i,date,h3_data,filtered))
            
        while True:
            if self.work_queue.empty() and self.write_queue.empty():
                self.work_queue.put(None)
                break
            sleep(1)

    def csv_to_numpy(self,full_path):
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

class Worker(mp.Process):
    def __init__(self,work_queue,write_queue):
        super().__init__()
        self.work_queue = work_queue
        self.write_queue = write_queue
        self.GRAVITY = 6.67430e-11

    def run(self):
        
        while True:
            results = []
            task = self.work_queue.get()
            if task == None:
                self.work_queue.put(None)
                self.write_queue.put(None)
                break
            
            num, date, h3_data, ephems = task
            for row in h3_data:
                results.append([
                    row[0], # geo_code
                    date,
                    row[1], # X
                    row[2], # Y
                    row[3], # Z
                    *self.process_row(row[1:],ephems) # gx,gy,gz,grav_mag
                ])
            self.write_queue.put((num,np.asarray(results)))

    def process_row(self,h3_coord,ephems):
        grav_coord = [0.0,0.0,0.0]
        
        for planet, mass in ephems:
            rel_coords = self.relative_coords(h3_coord, planet, mass)
            grav_coord = [
                grav_coord[0] + rel_coords[0],
                grav_coord[1] + rel_coords[1],
                grav_coord[2] + rel_coords[2]
            ]
        result = [*grav_coord,self.magnitude(grav_coord)]
        return result

    def relative_coords(self,coord, planet, mass):
        relative_coords = planet - coord
        mag = self.magnitude(relative_coords)
        grav_mag = self.gravity(mag,mass)
        grav_coord = self.scale_coord(relative_coords,mag,grav_mag)
        return grav_coord

    def magnitude(self,xyz):
        return (xyz[0]**2 + xyz[1]**2 + xyz[2]**2)**0.5

    def gravity(self,distance,mass):
        return self.GRAVITY*mass/distance**2

    def scale_coord(self,coord,distance,scaler):
        return [
            coord[0]/distance*scaler,
            coord[1]/distance*scaler,
            coord[2]/distance*scaler]

class Writer(mp.Process):
    def __init__(self,path,write_queue,chunk_size):
        super().__init__()
        self.path = path
        self.write_queue = write_queue
        self.chunk_size = chunk_size

    def run(self):
        array_list = []
        data_path = self.path+'/GravityData/'
        if not exists(data_path):
            makedirs(data_path)
        save_path = data_path+'gravity_{}.parquet'
        stats = {
            'local_total':0,
            'array_num':0,
            'part_num':0,
            'chunk_size':self.chunk_size,
            'path':save_path,
        }
        completed_dict = dict()
        while True:
            task = self.write_queue.get()
            if task == None:
                break
            order_num, array = task
            completed_dict[order_num] = array
            self.save_checks(stats,completed_dict,array_list)
        
        while len(completed_dict) > 0:
            self.save_checks(stats,completed_dict,array_list)

        if len(array_list) > 0:
            full_array = np.concatenate(array_list,axis=0)
            self.write_parquet(save_path,stats['part_num'],full_array)

    def save_checks(self,stats,completed_dict,array_list):
        if stats['array_num'] in completed_dict:
            next_array = completed_dict[stats['array_num']]
            if stats['local_total'] + len(next_array) > stats['chunk_size']:
                full_array = np.concatenate(array_list,axis=0)
                self.write_parquet(
                    stats['path'],
                    stats['part_num'],
                    full_array)
                print(f'Completed {stats['part_num']}')
                stats['local_total'] = len(next_array)
                array_list = [next_array]
                del completed_dict[stats['array_num']]
                stats['part_num'] += 1
            else:
                stats['local_total'] += len(next_array)
                array_list.append(next_array)
                del completed_dict[stats['array_num']]
            stats['array_num'] += 1

    def write_parquet(self,path,num,array:np.ndarray):
        table = pa.table({
            'geo_code':array[:,0],
            'datetime':array[:,1],
            'X':array[:,2].astype(dtype='d'),
            'Y':array[:,3].astype(dtype='d'),
            'Z':array[:,4].astype(dtype='d'),
            'gx':array[:,5].astype(dtype='d'),
            'gy':array[:,6].astype(dtype='d'),
            'gz':array[:,7].astype(dtype='d'),
            'grav_mag':array[:,8].astype(dtype='d')
        })
        pq.write_table(table,path.format(num))

def build_gravity_dataset(path,masses):
    # Getting dates from file
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
        workers.append(Worker(
            work_queue=work_queue,
            write_queue=write_queue))
    director = Director(
        path=path,
        datetimes=datetimes,
        masses=masses,
        work_queue=work_queue,
        write_queue=write_queue)
    writer = Writer(path=path,write_queue=write_queue,chunk_size=30e6)

    for worker in workers: worker.start()
    writer.start()
    director.start()
    for worker in workers: worker.join()
    director.join()
    writer.join()


if __name__ == '__main__':
    path = './Data'
    masses = {
        '10':1.989e30,
        'Mercury Barycenter':3.285e23,
        'Venus Barycenter':4.867e24,
        '301':7.347e22, # Earth's moon
        'Mars Barycenter': 6.39e23,
        'Jupiter Barycenter':1.898e27,
        'Saturn Barycenter':5.683e26,
        'Uranus Barycenter':8.681e25,
        'Neptune Barycenter':1.024e26
    }
    build_gravity_dataset(path=path,masses=masses)
    import pandas as pd
    ddf = pd.read_parquet('./Data/GravityData/')
    from graphing_data import create_3d_plot
    create_3d_plot(ddf)

    
