# -*- coding: utf-8 -*-
import requests
import datetime as dt
from constants import abel, openAQ_api_key, datafeeder, datafeeder_passwd


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
        Fetches the locations near Vienna.
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
        ------
        """
        urlLocations = "https://api.openaq.org/v2/locations?coordinates=48.20849%2C16.37208&radius=20000"
        response = requests.get(urlLocations,headers=self.headers)
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
        ------
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
        ------
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
        presents = (measurement[1] for measurement in measurements)
        return {
            str(parameterId) : parameters[parameterId] for parameterId in presents
        }
    def prepareLocations(self,locations):
        return {
            str(locationId) : locations[locationId] for locationId in locations
        }

class OpenAQ:
    def __init__(self):
        self.fetcher = DataFetcher()
        self.parameters = None
        self.locations = None
        self.latestMeasurements = None 
        self.lastUpdated = None 
    def connect(self):
        self.lastUpdated = dt.datetime.now()
        raw_locations = self.fetcher.fetchLocations()
        self.locations = self.fetcher.prepareLocations(raw_locations)
        raw_parameters = self.fetcher.fetchParameters()
        self.latestMeasurements = self.fetcher.fetchLatestMeasurements(raw_locations, raw_parameters)
        self.parameters = self.fetcher.prepareParameters(raw_parameters, self.latestMeasurements)

        

