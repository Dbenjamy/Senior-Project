from Earthquakes.earthquake_history import pull_earthquake_data, format_earthquake_data
from DataStructure.h3_data_structure import build_h3_index
from Ephemeris.ephem_requests import build_planets_ephems
from Ephemeris.pulling_gravity_data_together import build_gravity_dataset

if __name__ == '__main__':
    data_path = './Data'
    objects_and_masses = {
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

    # pull_earthquake_data()
    # format_earthquake_data()
    # build_h3_index(resolution=4,output_prefix='h3_index')
    build_planets_ephems(path=data_path,planet_ids=objects_and_masses.keys())
    build_gravity_dataset(path=data_path,masses=objects_and_masses)



# pull_earthquake_data(starttime='2019-01-01 00:00:00',endtime='2019-01-02 00:00:00',test=True)
