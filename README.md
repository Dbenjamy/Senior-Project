# Modeling Gravity for Earthquake CNN - Senior Capstone
The goal of this project is to pull, transform, and map ephemeris data (planet data) to model gravity on the surface of the Earth
## Technologies Used
* [h3](https://h3geo.org/) by Uber - 3.7.7
* [Astropy](https://www.astropy.org/) - 6.1.4
* [Horizons API](https://ssd.jpl.nasa.gov/horizons/) from NASA
* [Requests](https://pypi.org/project/requests/) - 2.32.3
* Python core libraries: Numpy, multiproccessing, threading
## Project Demo
You can either watch [this video of the demo](https://youtu.be/-nU9fxZI8H0) or run the project yourself with the following:

The demo will query planet ephemerides, transform that into gravity data, and graph it. You will need Python 3.12 or higher. After you clone the repository, you will need to install a few Python libraries:
```bash
python3 -m pip install -r requirements.txt
```
Then run `main`:
```bash
python3 main.py
```
## Layout
The project is organized into three directories:
- Data: where all generated data is stored
- DataStructure: code relevent to creating organizing data 
- Ephemeris: querying and transforming ephemeris data, ephemeris data being the locations of the main celestial bodies in our solar system
## Completed
- Data structure has been generated
- All relevent data for the model has been collected including ephemerides
- Gravity data for the major celestial bodies has been mapped to the Earth's surface
## In Progress
- Implement Python shared memory for more efficient data transformations
- Turn project into imporable Python package
