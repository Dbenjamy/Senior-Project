from astropy.time import Time
from astropy.coordinates import ITRS, GCRS, CartesianRepresentation

def eci_to_ecef(eci_coords, time_str):
    t = Time(time_str, scale='utc')

    # Format to ECI coordinates
    eci_rep = CartesianRepresentation(eci_coords)

    # Define GCRS coordinate (equivalent to ECI)
    gcrs = GCRS(eci_rep, obstime=t)

    # Convert GCRS to ITRS (equivalent to ECEF)
    itrs = gcrs.transform_to(ITRS(obstime=t))

    return [time_str, *itrs.cartesian.xyz.value]

if __name__ == '__main__':
    # Example ECI coordinates and time
    eci_coords = [1.0, 2.0, 3.0]  # in kilometers
    time_str = '2024-01-01 00:00:00'

    # Convert ECI to ECEF
    ecef_coords = eci_to_ecef(eci_coords, time_str)
    print('ECEF Coordinates:', ecef_coords)
