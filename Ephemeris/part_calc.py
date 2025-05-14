import numpy as np
from multiprocessing import shared_memory
import numpy.typing as npt
def write_split(assignments:np.Queue[int,tuple[str,tuple]]):
    row_num, mem_locs = assignments.get()
    work_done:npt.NDArray = list(range(row_num-row_num+5))
    for name, shape in mem_locs:
        shared_memory.SharedMemory(name=name,)
        pass

def split_schedule(
        start_size:int,size_per_row:int,chunk_size:int,final_row_count:int,
        calc_row_count:int,name:str="calc_") -> tuple[tuple]:
    
    if start_size + size_per_row > chunk_size:
        raise ValueError("chunk_size too small.")
    
    schedule = []
    curr_size = start_size
    calc_to, calc_from, calc_count = 0
    partition = []
    for _ in range(final_row_count):
        chunk_limit = curr_size + size_per_row > chunk_size
        end_of_calc = calc_to+1 == calc_row_count

        if chunk_limit and end_of_calc:
            partition.append((name+str(calc_count),calc_from,calc_to))
            schedule.append(tuple(partition[:]))
            partition = []
            curr_size = start_size
            calc_from, calc_to = 0
            calc_count += 1
        elif chunk_limit:
            partition.append((name+str(calc_count),calc_from,calc_to))
            schedule.append(tuple(partition[:]))
            partition = []
            curr_size = start_size
            calc_from = calc_to
        elif end_of_calc:
            partition.append((name+str(calc_count),calc_from,calc_to))
            calc_from, calc_to = 0
            calc_count += 1

        curr_size += size_per_row
        calc_to += 1

    if len(partition) != 0:
        partition.append((name+str(calc_count),calc_from,calc_to))
        schedule.append(tuple(partition[:]))
    
    return schedule
