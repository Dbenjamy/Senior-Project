import h3
from os.path import exists
from os import makedirs

def h3_index_generator(parents, resolution=1):
    for parent in parents:
        for item in h3.h3_to_children(parent, resolution):
            index = str(item)
            lat, lon = [str(coord) for coord in h3.h3_to_geo(item)]
            yield ','.join((index,lat,lon))

def create_index_file(file_save_path,chunk_size,gen):
    end = False
    with open(file_save_path,'w') as file:
        file.write('geo_code,lat,lon\n')
        for _ in range(chunk_size):
            try:
                file.write(str(next(gen))+'\n')
            except StopIteration:
                end = True
    return end

def build_h3_index(
        path='./Data',
        resolution=3,
        chunk_size=2000000,
        output_prefix='output'):
    
    if not exists(path+'/h3Index'):
        makedirs(path+'/h3Index')

    parents = h3.get_res0_indexes()
    gen = h3_index_generator(parents,resolution=resolution)
    counter = 0
    while True:
        file_save_path = f"{path}/h3Index/{output_prefix}_{counter}.csv"
        end = create_index_file(
            file_save_path=file_save_path,
            chunk_size=chunk_size,
            gen=gen)
        if end:
            break
        counter += 1

if __name__ == '__main__':
    build_h3_index(resolution=4,output_prefix='h3_index')