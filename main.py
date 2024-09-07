from Earthquakes.earthquake_history import pull_earthquake_data, mapping_dates_to_ranges
from DataStructure.h3_data_structure import build_h3_index
from Ephemeris.ephem_requests import build_planets_ephems

if __name__ == '__main__':
    path = './Data'


# pull_earthquake_data(starttime='2019-01-01 00:00:00',endtime='2019-01-02 00:00:00',test=True)