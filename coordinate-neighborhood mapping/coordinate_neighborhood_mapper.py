import json
from shapely.geometry import Point, Polygon
import time
import datetime
import numpy as np
import dask.dataframe as dd


#This function takes the longitude and latitude of a pickup or dropoff location and maps it to a neighborhood (or state, county, etc.).
#Take note that this function uses dask dataframes instead of pandas dataframes becuase they are much quicker with computations.


def neighborhood_name_adder(data, geo_data, column_longitude, column_latitude, new_column_name):
    """
    data : Dask data frame of taxi trips
    geo_data: GeoJson containing the shape of each neighborhood
    column_longitude: Name of the longitude column in the dataset
    column_latitude: Name of the latitude column in the dataset
    new_column_name: Name of the new column (i.e. `pickup_neighborhood`)
    """
    from matplotlib import path
    import numpy as np
       
    
    lat = data[column_latitude]      # selecting longitude from dask dataframe
    lon = data[column_longitude]     # selecting latitude from dask dataframe
    
    data[new_column_name] = np.zeros(len(data)) # iniitalizing new column
    
    # The loop below goes through every neighborhood in the GeoJson file and maps \
    # \the neighborhood name to the new column if a trip is within taht neighborhood

    
    for feature in geo_data['features']:     # looping through every neighborhood in GeoJson file, and mapping all trips
        coords = feature['geometry']['coordinates'][0]
        p = path.Path(coords)
        index = p.contains_points(zip(lon, lat))
        data[new_column_name].loc[index] = [str(feature['properties']['neighborhood'])]*np.sum(index)

    
    return(data)

# An example of how to run this code is below. The csv file I use in this example 
# \ is posted in this repository, as is the GeoJson file `NY_neighborhoods.geojson`

taxi_trips = dd.read_csv('Taxi_green_2013.csv')  # loading in data using dask

print ("old data set columns are: ", taxi_trips.columns)

geo_data =  json.load(open('NY_neighborhoods.geojson'))   # GeoJson file

taxi_trips_updated = neighborhood_name_adder(taxi_trips, geo_data, 'Pickup_longitude', 'Pickup_latitude', 'Pickup_neighborhood')

print ("updated data set columns are: ", taxi_trips_updated.columns)

