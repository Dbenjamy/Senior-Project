# Earthquake Prediction CNN - Senior Capstone
The goal of this project is to work though the core workflow of any machine learning model:
1. Backround research and initial testing
2. Collecting and parsing data
3. Creating the data structure to be used in training the model
4. Choosing/implementing hyperparameters for the model and formatting the data for training
5. Training and validating the results of the model

The project is organized into four directories:
- Data: where all generated data is stored
- DataStructure: code relevent to creating the data structure
- Earthquakes: querying and organizing earthquake data
- Ephemeris: querying and transforming ephemeris data, ephemeris data being the locations of the main celestial bodies in our solar system
### Completed
The data structure has been generated. And all relevent data for the model has been collected. Gravity data for the major celestial bodies has been mapped to the Earth's surface.
### In Progress
Gravity data from all planets need to be generated and added together to map the net affect of gravity on Earth. After that I need to:
- Join gravity and earthquake data into single table
- Construct CNN model and design traversal algorithm for my data structure
- Decide on hyperparameters; I'm thinking I'll use the cyclical focal loss function since there are many empty data points
- Train model and analyse the results
