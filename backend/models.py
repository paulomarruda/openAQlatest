# -*- coding: utf-8 -*-
'''
Data dealer and fetcher for the OpenAQ API. 
'''
import requests
import datetime as dt
from constants import abel, openAQ_api_key, datafeeder, radius, vienna_coordinates


class DataFetcher:
    """
    Fetcher for the data provided by the OpenAQ API. This will function as the 
    model.

    Attributes
    ----------
    headers : dict
        A dictionary containing the API-KEY allowing access to the API.
    dt_format_in : str
        A string with the format of the time used by OpenAQ.
    initial_cut : datetime.datetime
        Initial date to cut off the locations that are not receiving more updates.
    Methods
    -------
    fetchLocations -> list
        Fetches the locations near Vienna (20 km radius).
    fetchParameters -> list
        Fetches the parameters used by OpenAQ.
    fetchLatestMeasurements -> list
        Fetches the latest data of the provided locations.
    """

    def __init__(self):
        self.headers = {"X-API-Key": openAQ_api_key}
        self.dt_format_in = "%Y-%m-%dT%H:%M:%S+00:00"
        today = dt.date.today()
        self.initial_cut = dt.datetime(
            year = today.year,
            month = today.month,
            day = today.day
        )

    def fetchLocations(self) -> dict:
        """
        Attributes
        ----------
        Returns
        -------
        locations : list
        A dictionary containing the locations near Vienna:
            locationID (key): int 
                The unique identifier of the location.
            name : str
                Name of the location.
            latitude : float
                Latitude of the location.
            longitude: float 
                Longitude of the location.
            modelName : str
                Name of the model
            manufacturerName : str
                Name of the manufacturer          
        Raises
        ======
        TBI
        """
        urlLocations = f"https://api.openaq.org/v2/locations?coordinates={vienna_coordinates.get('latitude')}%2C{vienna_coordinates.get('longitude')}&radius={radius}"
        response = requests.get(urlLocations, headers=self.headers)
        # implement dealing with responses 
        results = response.json().get("results")
        locations = {}
        for location in results:
            lastupdated = dt.datetime.strptime(location.get("lastUpdated"), self.dt_format_in)
            if lastupdated >= self.initial_cut:
                locations[location.get("id")] = {
                    "name": location.get("name"),
                    "firstUpdated" : location.get("firstUpdated"),
                    "lastUpdated" : location.get("lastUpdated"),
                    "latitude": location.get("coordinates").get("latitude"),
                    "longitude": location.get("coordinates").get("longitude"),
                    "modelName": location.get("manufacturers")[0].get("modelName"),
                    "manufacturerName": location.get("manufacturers")[0].get("manufacturerName"),
                }
        return locations

    def fetchParameters(self) -> dict:
        """
        Attributes
        ----------
        Returns
        -------
        locations : dict 
            A dictionary containing the parameters used by OpenAQ:
            parameterID (key): int 
                The unique identifier of the parameter.
            name : str
                Name of the location.
            displayName : str
                Display name for the paramter.
            Description: str 
                Description of the parameter.
            preferredUnit : str
                Unit of the measurement collected by this parameter.
        Raises
        ======
        TBI
        """
        url = "https://api.openaq.org/v2/parameters"
        response = requests.get(url, headers=self.headers)
        total_parameters = response.json().get("results")
        parameters = {}
        for parameter in total_parameters:
            parameterId = parameter.pop("id")
            parameters[parameterId] = parameter 
        return parameters


    def fetchLatestMeasurements(self, locations : list, parameters : list) -> list:
        """
        Attributes
        ----------
        locations : dict 
            A dictionary containing each location: 
            locationId (key): int 
                The unique identifier of the location.
            name : str
                Name of the location.
            latitude : float
                Latitude of the location.
            longitude: float 
                Longitude of the location.
            lastUpdated : datetime.datetime
                Time of the last update (UTC).
            firstUpdated : datetime.datetime
                Time of the first update (UTC).
            modelName : str
                Name of the model
            manufacturerName : str
                Name of the manufacturer
        parameters : list 
            A dictionary containing the parameters used by OpenAQ:
            parameterID (key) : int 
                The unique identifier of the parameter.
            name : str
                Name of the parameter.
            displayName : str
                Display name for the paramter.
            Description: str 
                Description of the parameter.
            unit : str
                Unit of the measurement collected by this parameter.
        Returns
        -------
        latestMeasurements : list
            A list containing the lastest measurements collected in each location. Each measurement is a tuple containing in this order:
            lastUpdated : datetime.datetime
                The unique identifier of the parameter.
            parameterId : int
                Unique identifier of the parameter measuremed.
            locationId : int
                Unique identifier of the location of the measurement.
            value: float 
                Value of the measurement.
        Raises
        ======
        TBI
        """
        url = "https://api.openaq.org/v2/parameters"
        dict_locations = {locations.get(locationId).get("name") : locationId for locationId in locations}
        dict_parameters = {parameters.get(parameterId).get("name") : parameterId for parameterId in parameters}
        url = "https://api.openaq.org/v2/latest?"
        for locationId in locations:
            url = url + f"&location={locationId}"
        response = requests.get(url, headers=self.headers)
        results = response.json().get('results')
        measurements = []
        for result in results:
            location_id = dict_locations.get(result.get('location'))
            raw_measurements = result.get('measurements')
            for measurement in raw_measurements:      
                measurements.append(
                    (
                    dt.datetime.strptime(measurement.get('lastUpdated'), self.dt_format_in),
                    dict_parameters.get(measurement.get('parameter')),
                    location_id,
                    measurement.get('value')
                    )
                )
        return measurements
    def prepareParameters(self, parameters, measurements):
        '''
        Parser for the parameters keys. Turn the keys from `int` to `str`
        and select only the parameters being measured by the present locations.
        Parameters
        ----------
        parameters: dict
            the dictionary contaning the parameters and their info.
        measurements : tuple
            latest measurements collected.
        Returns
        -------
        parameters: dict
            the dictionary with keys turned into strings.
        '''
        presents = (measurement[1] for measurement in measurements)
        return {
            str(parameterId) : parameters[parameterId] for parameterId in presents
        }
    def prepareLocations(self,locations):
        '''
        Pareser for the locations keys. Turn the keys from `int` to `str`.
        Parameters
        ----------
        locations: dict
            the dictionary containing the locations and their info. 
        Returns
        -------
        locations: dict 
            the dictionary with keys turned into strings.
        '''
        return {
            str(locationId) : locations[locationId] for locationId in locations
        }
