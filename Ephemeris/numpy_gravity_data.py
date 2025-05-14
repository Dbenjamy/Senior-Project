from datetime import datetime
from multiprocessing.shared_memory import SharedMemory
from multiprocessing.sharedctypes import Synchronized
from os import makedirs
from os.path import exists
from time import sleep, time
import csv
import multiprocessing as mp
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import numpy.typing as npt

class Director(mp.Process):
    def __init__(self,
            datetimes:npt.NDArray,work_queue:mp.Queue,
            write_queue:mp.Queue,idle_time:int):
        super().__init__()
        self.datetimes = datetimes
        self.work_queue = work_queue
        self.write_queue = write_queue
        self.idle_time = idle_time

    def run(self):
        for i, date in enumerate(self.datetimes):
            start = time()
            self.work_queue.put((i,date))
            self.idle_time.value += time()-start
        # Cleanup
        while True:
            if self.work_queue.empty() and self.write_queue.empty():
                self.work_queue.put(None)
                break
            sleep(1)

class Worker(mp.Process):
    def __init__(self,
            work_queue:mp.Queue,work_barrier:mp.Barrier,
            masses_spec:dict,ephems_spec:dict,h3_spec:dict,
            idle_get,idle_put,mem_name:str="calc_"):
        super().__init__()
        self.work_queue = work_queue
        self.work_barrier = work_barrier
        self.masses_spec = masses_spec
        self.ephems_spec = ephems_spec
        self.h3_spec = h3_spec
        self.idle_get = idle_get
        self.idle_put = idle_put
        self.mem_name = mem_name
        self.__data_init__()

    def __data_init__(self):
        self.GRAVITY = 6.67430e-11
        self.masses, self.masses_sh = build_shared_numpy(**self.masses_spec,read_only=True)
        self.ephems, self.ephems_sh = build_shared_numpy(**self.ephems_spec,read_only=True)
        self.h3_data, self.h3_sh    = build_shared_numpy(**self.h3_spec,read_only=True)
        self.output_dtype = [
            ("geo_code","U15"),
            ("timestamp","datetime64[ns]"),
            ("X",np.float64),
            ("Y",np.float64),
            ("Z",np.float64),
            ("gx",np.float64),
            ("gy",np.float64),
            ("gz",np.float64),
            ("grav_mag",np.float64),
        ]
    
    def run(self):
        while True:
            results = []
            start = time()
            task = self.work_queue.get()
            self.idle_get.value += time()-start
            if task == None:
                self.work_queue.put(None)
                self.work_barrier.wait()
                self.write_queue.put(None)
                break
            
            
            num, date = task

            for row in self.h3_data:
                results.append([
                    row[0], # geo_code
                    date,
                    row[1], # X
                    row[2], # Y
                    row[3], # Z
                    *self.process_row(row[1:],date) # gx,gy,gz,grav_mag
                ])
            start = time()
            self.write_queue.put(np.asarray(results))
            self.idle_put.value += time()-start
    
    def process_row(self,h3_coord):
        grav_coord = [0.0,0.0,0.0]
        for planet, mass in ephems:
            rel_coords = self.gravity_coords(h3_coord, planet, mass)
            grav_coord = [
                grav_coord[0] + rel_coords[0],
                grav_coord[1] + rel_coords[1],
                grav_coord[2] + rel_coords[2]
            ]
        return (*grav_coord,self.magnitude(grav_coord))

    def one_moment_from_all_planets_at_all_locations(
            self,count:int,current:int=0,start:int=1,stop:int=4):
        # if current == count:
        #     return one_moment_from_one_planet_at_all_locations(s)
        # one_moment(self.ephems[])
        pass

    def one_moment(self,ephems,mass):
        relative = self.h3_data[:,1:4] - ephems
        distance = np.linalg.norm(relative)
        grav_mag = self.GRAVITY*mass/distance**2
        return relative/distance*grav_mag

    # def gravity_coords(self,coord, planet, mass):
    #     relative_coords = planet - coord
    #     mag = self.magnitude(relative_coords)
    #     grav_mag = self.gravity(mag,mass)
    #     grav_coord = self.scale_coord(relative_coords,mag,grav_mag)
    #     return grav_coord

    # def magnitude(self,xyz:list[float]) -> float:
    #     return (xyz[0]**2 + xyz[1]**2 + xyz[2]**2)**0.5

    # def gravity(self,distance,mass):
    #     return self.GRAVITY*mass/distance**2

    # def scale_coord(self,coord,distance,scaler):
    #     return [
    #         coord[0]/distance*scaler,
    #         coord[1]/distance*scaler,
    #         coord[2]/distance*scaler]

    # def filter_all_by_date(self,ephems:list[np.ndarray],date):
    #     filtered = []
    #     for planet, mass in ephems:
    #         planet_filtered = planet[planet[:,0]==date][:,1:][0]
    #         filtered.append((planet_filtered,mass))
    #     return filtered

class Writer(mp.Process):
    def __init__(self,
            path:str,
            write_queue:mp.Queue,
            chunk_size:int,
            idle_times,
            output_length:int=None):
        super().__init__()
        self.path = path
        self.write_queue = write_queue
        self.chunk_size = chunk_size
        self.output_length = output_length
        self.idle_times = idle_times
        if type(self.output_length) == int:
            self.part_count = int(output_length//chunk_size)

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
            start = time()
            task = self.write_queue.get()
            self.idle_times['writer_get'].value += time()-start
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
            'geo_code':self.slicer_vectorized(array[:,0],8),# Cut last 8 'f's
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
        self.report_progress(num)

    def slicer_vectorized(self,arr,end):
        slicer = np.vectorize(lambda x: x[:-end])
        return slicer(arr)

    def report_progress(self,part_num):
        if type(self.output_length) == int:
            first_line = f'Completed {part_num}/{self.part_count}\n'
        else:
            first_line = f'Completed {part_num}\n'
        print(first_line
        + 'Idle Times:\n'
        +f'\tDirector Put: {round(self.idle_times["director_put"].value,2)}\n'
        +f'\tWorkers Get:  {round(self.idle_times["worker_get"].value,2)}\n'
        +f'\tWorkers Put:  {round(self.idle_times["worker_put"].value,2)}\n'
        +f'\tWriter Get:   {round(self.idle_times["writer_get"].value,2)}',
        flush=True)

def build_gravity_dataset(path,planet_entries_dict):
    # Getting dates from file
    planet_entries = format_planet_entries(
        path+'/EphemData/{}_ephem.csv',
        planet_entries_dict)
    masses, ephems = group_planet_data(planet_entries)
    h3_data = load_h3_data(path)
    # shared_masses, sh_masses = build_shared_numpy("masses",masses)
    # shared_ephems, sh_ephems = build_shared_numpy("ephems",ephems)
    # shared_h3, sh_h3         = build_shared_numpy("h3_data",h3_data)
    # build_shared_numpy("h3_data",h3_data)
    print("\n")
    infer_2D_dtype(ephems)
    print("\n")
    return None

    
    datetimes = shared_ephems[:,0]
    CPU_COUNT = mp.cpu_count()
    work_queue  = mp.Queue(maxsize=CPU_COUNT*2)
    write_queue = mp.Queue(maxsize=CPU_COUNT*2)
    # write_queue = mp.Manager().Queue(maxsize=CPU_COUNT*2)
    # Tracking idle time
    idle_times = {
        'director_put': mp.Value('f',0),
        'worker_get': mp.Value('f',0),
        'worker_put': mp.Value('f',0),
        'writer_get': mp.Value('f',0)
    }
    director = Director(
        path=path,
        datetimes=datetimes,
        planet_entries_dict=planet_entries_dict,
        work_queue=work_queue,
        write_queue=write_queue,
        idle_time=idle_times['director_put'])

    work_barrier = mp.Barrier(CPU_COUNT-2)
    workers = []
    for _ in range(CPU_COUNT-2):
        workers.append(Worker(
            work_queue=work_queue,
            write_queue=write_queue,
            work_barrier=work_barrier,
            masses_spec=pack_numpy_spec(masses,"masses"),
            ephems_spec=pack_numpy_spec(ephems,"ephems"),
            h3__spec=pack_numpy_spec(h3_data,"h3_data"),
            idle_get=idle_times['worker_get'],
            idle_put=idle_times['worker_put']))

    output_length = len(pq.read_table(path+'/h3Index/'))*len(datetimes)

    writer = Writer(
        path=path,
        write_queue=write_queue,
        chunk_size=30e6,
        idle_times=idle_times,
        output_length=output_length)

    print('Starting Gravity Calculations',flush=True)
    director.start()
    for worker in workers: worker.start()
    writer.start()

    director.join()
    for worker in workers: worker.join()
    writer.join()

def build_shared_numpy(name:str,
        data:npt.NDArray=None,
        size:int=None,
        shape:tuple[int]=None,
        dtype:npt.DTypeLike=None,
        read_only=False
        ) -> tuple[npt.NDArray,SharedMemory]:
    if data is None and any((dtype is None),(size is None),(shape is None)):
        raise ValueError("Trying to load shared Numpy array without specifications.")
    if data is None:
        local_size  = size 
        local_shape = shape
        local_dtype = dtype
    else:
        local_size  = data.size
        local_shape = data.shape
        local_dtype = data.dtype#infer_2D_dtype(data)
    sh = SharedMemory(name,create=True,size=local_size)
    print(sh.size)
    print(data.size)
    shared_data = np.ndarray(shape=local_shape,dtype=local_dtype,buffer=sh.buf)
    # shared_data[...] = data[...]
    # if read_only:
    #     shared_data.flags.writeable = False
    # return shared_data, sh

def pack_numpy_spec(data:npt.NDArray,name:str) -> dict:
    dtype = tuple(("col_"+str(i),np.array([data[0,i]]).dtype)
        for i in range(data.shape[1]))
    return {"name":name,"size":data.size,"shape":data.shape,"dtype":dtype}

def infer_2D_dtype(data:npt.NDArray):
    for i in range(data.shape[1]):
        print(np.array(data[:,i])[0])
        break
    return tuple(("col_"+str(i),np.array([data[0,i]]).dtype)
        for i in range(data.shape[1]))


def group_planet_data(planet_entries:tuple[str,float]):
    ephems = []
    masses = np.array([num for _,num in planet_entries])    
    ephems.append(csv_to_numpy(planet_entries[0][0])[:,0])
    # dtype = [('timestamp','datetime64[ns]')]
    for file, _ in planet_entries:
        planet_array = csv_to_numpy(file)[:,1:4]
        ephems.append(planet_array)
        # dtype = dtype + [
        #     ('X'+str(i),'float64'),
        #     ('Y'+str(i),'float64'),
        #     ('Z'+str(i),'float64')]
    return masses, np.column_stack(ephems)#, dtype

def load_h3_data(path:str):
    return np.column_stack([col.to_numpy()
        for col in pq.read_table(path+'/h3Index/').columns])

def format_planet_entries(path:str,entries:dict[str,float]):
    return tuple(
        (path.format(obj_id.replace(' ','_')),mass)
        for obj_id,mass in entries.items())

def csv_to_numpy(full_path) -> list[np.ndarray]:
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


if __name__ == '__main__':
    path = './Data'
    # build_sharable_combined_data(path+"/EphemData")
    planet_entries_dict = {
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
    build_gravity_dataset(path,planet_entries_dict)
    # for data in group_planet_data(path,planet_entries_dict)[:2]:
    #     # for item in data:
    #     #     print(item)
    #     #     break
    #     if prev is None:
    #         prev = data[0][:,1:-1]
    #     else:
    #         print('YOOO')
    #         print(np.column_stack([data[0][:,0],prev]))

    #     print("\n")
    # print(len(group_planet_data(path,planet_entries_dict)))
    # print(pq.read_table("./Data/h3Index/h3_index_0.parquet"))
    # print(prev)
#     build_gravity_dataset(path=path,planet_entries_dict=planet_entries_dict)
#     import pandas as pd
#     from graphing_data import create_3d_plot
#     ddf = pd.read_parquet('./Data/GravityData/gravity_0.parquet')
#     create_3d_plot(ddf)

