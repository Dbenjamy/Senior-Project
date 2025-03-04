# Modeling Gravity for Earthquake CNN - Senior Capstone
The goal of this project is to work though the core workflow of any machine learning model:
1. Backround research and initial testing
2. Collecting and parsing data
3. Creating the data structure to be used in training the model
4. Choosing/implementing hyperparameters for the model and formatting the data for training
5. Training and validating the results of the model
## Technologies Used
* [h3](https://h3geo.org/) by Uber - 3.7.7
* [Astropy](https://www.astropy.org/) - 6.1.4
* [Dask](https://www.dask.org/) - 2024.9.0
* [Horizons API](https://ssd.jpl.nasa.gov/horizons/) from NASA
* [Requests](https://pypi.org/project/requests/) - 2.32.3
* Python core libraries: multiproccessing, threading, numpy
## Project Demo
The demo will query planet ephemerides and graph it as gravity data. You will need Python 3.12 or higher. After you clone the repository, you will need to install a few Python libraries:
```bash
python3 -m pip install -r requirements.txt
```
Then run `main`:
```bash
python3 main.py
```
## Layout
The project is organized into four directories:
- Data: where all generated data is stored
- DataStructure: code relevent to creating the data structure
- Earthquakes: querying and organizing earthquake data
- Ephemeris: querying and transforming ephemeris data, ephemeris data being the locations of the main celestial bodies in our solar system
## Completed
The data structure has been generated. And all relevent data for the model has been collected. Gravity data for the major celestial bodies has been mapped to the Earth's surface.
## In Progress
- Design traversal algorithm for data
- Join gravity and earthquake data into single table
- Construct CNN model
- Decide on hyperparameters; thinking I'll use the cyclical focal loss function since there are many empty data points
- Train model and analyse the results
