
def magnitude(xyz_dataframe):
    return np.sqrt(
        np.square(xyz_dataframe['X'])
        + np.square(xyz_dataframe['Y'])
        + np.square(xyz_dataframe['Z']))

def gravity(distance,mass):
    return G*mass/np.square(distance)

def gravity_coords(coords,mass):
    distance = magnitude(coords)
    grav_mag = gravity(distance,mass)
    grav_coords = dd.concat([
        coords['X']/distance*grav_mag,
        coords['Y']/distance*grav_mag,
        coords['Z']/distance*grav_mag
        ],axis=1
        ).rename(columns={0:'gx',1:'gy',2:'gz'})
    
    return grav_coords
#########################################################

def rel_coords_generator(coords,planet_data,mass):
    relative_coords = np.array(
        (-1*coords+planet_data[:,1:]).tolist(),
        dtype='float64')
    distance = np.sqrt(
        np.square(relative_coords[:,0])
        + np.square(relative_coords[:,1])
        + np.square(relative_coords[:,2]))
    grav_coords = relative_coords/distance.reshape(-1,1)*gravity_scaler(relative_coords,mass).reshape(-1,1)
    
    return np.concatenate(
        [planet_data[:,0].reshape(-1,1),grav_coords],
        axis=1)


if __name__ == '__main__':
    
    planet_data = pd.read_csv('./Data/EphemData/301_ephem.csv').to_numpy()
    h3_data = pd.read_csv('./Data/h3Index/h3_index_0.csv')
    h3_data['X'] = R_earth*np.sin(h3_data['lat'])*np.cos(h3_data['lon'])
    h3_data['Y'] = R_earth*np.sin(h3_data['lat'])*np.sin(h3_data['lon'])
    h3_data['Z'] = R_earth*np.cos(h3_data['lat'])


    coords = h3_data[['X','Y','Z']].to_numpy()
    mass_of_moon = 7.34767309e22
    
    # Generator for surface points and their relative distances
    data_gen = (
        [h3_row[0],
        h3_row[1],
        h3_row[2],
        h3_row[3],
        rel_coords_generator(
            np.array([h3_row[1:]]),
            planet_data,
            mass_of_moon).tolist()
        ]
        for h3_row in h3_data[['geo_code','X','Y','Z']].to_numpy())
    
    new_data = []
    done = False
    for row in tuple(data_gen):
        for entry in row[-1]:
            grav_mag = (entry[1]**2 + entry[2]**2 + entry[3]**2)**0.5
            new_data.append(
                [row[0]]
                + [entry[0]]
                + row[1:-1]
                + entry[1:]
                + [grav_mag])
    df = pd.DataFrame(
            new_data,
            columns=[
                'geo_code',
                'datetime',
                'X','Y','Z',
                'gx','gy','gz',
                'grav_mag']
        ).set_index(['geo_code','datetime'])
    print(df)