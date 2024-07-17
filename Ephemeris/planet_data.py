import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from astropy.constants import G

def gravity_scaler(xyz,mass:int):
    X, Y, Z = xyz[:,0], xyz[:,1], xyz[:,2]
    # Calculate distance from each vector
    distance = np.sqrt(np.square(X) + np.square(Y) + np.square(Z))
    # Calculate gravitational force for each vector
    return (G*mass/np.square(distance)).value


if __name__ == '__main__':
    df = pd.read_csv('./Data/EphemData/Mars_Barycenter_ephem.csv')
    df[['X','Y']].plot(kind='scatter',x='X',y='Y')
    plt.show()
    data = df[['X','Y','Z']].to_numpy()
    mass_of_moon = 7.34767309e22
    force_in_newtons = gravity_scaler(data,mass_of_moon)
    for item in force_in_newtons: print(round(item,4))
