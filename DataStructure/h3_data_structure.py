import h3
import dask

def data_generator(parents, resolution=1):
    for parent in parents:
        for item in h3.h3_to_children(parent, resolution):
            index = str(item)
            lat, lon = [str(coord) for coord in h3.h3_to_geo(item)]
            yield ','.join((index,lat,lon))

def save_data_in_chunks(parents, chunk_size=2000000, output_prefix='output'):
    counter = 0
    gen = data_generator(parents,resolution=3)

    while True:
        end = False
        with open(f"./Data/h3Index/{output_prefix}_{counter}.csv",
                  'w') as file:
            file.write('geo_code,lat,lon\n')
            for _ in range(chunk_size):
                try:
                    file.write(str(next(gen))+'\n')
                except StopIteration:
                    end = True
                    break
        if end:
            break
        counter += 1


parents = h3.get_res0_indexes()  # Your list of parents
save_data_in_chunks(parents,output_prefix='h3_index')