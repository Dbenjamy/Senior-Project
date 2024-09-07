from Earthquakes.earthquake_history import pull_earthquake_data, format_earthquake_data
from DataStructure.h3_data_structure import build_h3_index
from Ephemeris.ephem_requests import build_planets_ephems

if __name__ == '__main__':
    path = './Data'
    # pull_earthquake_data()
    # format_earthquake_data()
    # build_h3_index(resolution=4,output_prefix='h3_index')
    build_planets_ephems()



# pull_earthquake_data(starttime='2019-01-01 00:00:00',endtime='2019-01-02 00:00:00',test=True)