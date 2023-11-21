import numpy as np
import pandas as pd
import numpy as np
from datetime import datetime as dt
from shapely.geometry import Point
import geopandas as gpd
import numpy as np
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

def get_zone_info(shpfile_path, longitude, latitude)->dict:
    """
    Retrieve zone information based on a given point's longitude and latitude.

    Args:
        shpfile_path (str): Path to the shapefile.
        longitude (float): Longitude of the point.
        latitude (float): Latitude of the point.

    Returns:
        dict: A dictionary containing zone information.
            - zoneID: 6-digit zone ID as in the shapefile.
            - zoneName: Name of the zone as in the shapefile.
    """
    

    # read shapefile
    gdf = gpd.read_file(shpfile_path)
    
    # convert to WGS84
    new_gdf = gdf.to_crs("EPSG:4326", inplace=False)
    
    zoneInfo = {}
    # retrieve zoneID and zoneDescription, and ZoneName for a given point(Longitude, Latitude)
    for i in range(len(new_gdf)):
        polygon = new_gdf.loc[i,'geometry']
        if polygon.contains(Point(longitude, latitude)):
            zoneID = new_gdf.loc[i,'zoneID']
            zoneName = new_gdf.loc[i,'zoneName']
            zoneDescription = new_gdf.loc[i,'zoneDescription']
            zoneInfo['zoneID'] = [zoneName, zoneDescription]
            break
    
    return zoneInfo

# calculate haversine distance in gm_data given startpositionlat, startpositionlng, endpositionlat, endpositionlng
def haversine_distance(startLat, startLng, endLat, endLng):
    
    # approximate radius of earth in km
    R = 6373.0

    startLat = np.radians(startLat)
    startLng = np.radians(startLng)
    endLat = np.radians(endLat)
    endLng = np.radians(endLng)

    dlon = endLng - startLng
    dlat = endLat - startLat

    a = np.sin(dlat / 2)**2 + np.cos(startLat) * np.cos(endLat) * np.sin(dlon / 2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    distance = R * c
    return distance





# function to undate the start date based on the weekday
def update_start_date_vectorized(weekdays, start_dates):
    """
    Update the start dates by finding the last occurrences of the weekdays and combining them with the corresponding times.

    Args:
        weekdays (Series): A pandas Series containing the weekdays.
        start_dates (Series): A pandas Series containing the start dates.

    Returns:
        np.ndarray: An array of updated dates and times.

    """
    # create dictionary of weekday and the dates for last week with weekday name being key, and date being value
    last_week = {}
    for i in range(7):
        date = dt.today().date() - pd.Timedelta(days=i)
        last_week[date.strftime('%A')] = date

    # Find the dates for the last occurrences of the weekdays
    update_dates = weekdays.map(last_week)
    # Extract the time from the start_dates
    times = start_dates.dt.time
    # Combine the last occurrence dates with the corresponding times
    updated_dates = np.vectorize(dt.combine)(update_dates, times)

    return updated_dates



def get_top_itineraries(from_lat, from_lon, to_lat, to_lon, datetime_obj, 
                        mode ='TRANSIT', arrive_by=False, wheelchair=False, 
                        show_intermediate_stops=True, locale='en', num_itineraries=3):
    """
    Get the top itineraries for a given origin and destination using the OpenTripPlanner API.

    Parameters:
    - from_lat (float): Latitude of the origin location.
    - from_lon (float): Longitude of the origin location.
    - to_lat (float): Latitude of the destination location.
    - to_lon (float): Longitude of the destination location.
    - datetime_obj (datetime.datetime): Date and time of the trip.
    - mode (str, optional): Mode of transportation. Defaults to 'TRANSIT'.
    - arrive_by (bool, optional): Whether to arrive by the specified time. Defaults to False.
    - wheelchair (bool, optional): Whether to consider wheelchair accessibility. Defaults to False.
    - show_intermediate_stops (bool, optional): Whether to show intermediate stops. Defaults to True.
    - locale (str, optional): Locale for the response. Defaults to 'en'.
    - num_itineraries (int, optional): Number of top itineraries to retrieve. Defaults to 3.

    Returns:
    - dict: JSON response containing the top itineraries.
    """
    
    # Format the date and time
    date = datetime_obj.strftime('%Y-%m-%d')  # Format: YYYY-MM-DD
    time = datetime_obj.strftime('%H:%M')     # Format: HH:MM (24-hour)

    # Construct the URL with query parameters
    url = (
        'http://localhost:8080/otp/routers/default/plan'
        '?fromPlace={},{}'
        '&toPlace={},{}'
        '&time={}'
        '&date={}'
        '&mode={}'
        '&arriveBy={}'
        '&wheelchair={}'
        '&showIntermediateStops={}'
        '&locale={}'
        '&numItineraries={}'
        '&optimize=QUICK'  # Ensure the fastest route is prioritized
    ).format(
        from_lat, from_lon, 
        to_lat, to_lon, 
        time, 
        date, 
        mode, 
        str(arrive_by).lower(), 
        str(wheelchair).lower(), 
        str(show_intermediate_stops).lower(), 
        locale,
        num_itineraries)
    # Send the request
    response = requests.get(url)
    return response.json()

# function to get the fastest itinerary
def fetch_and_process_fastest_itinerary(response):
    """
    Fetches and processes the fastest itinerary from the response.

    Args:
        response (dict): The response containing itinerary information.

    Returns:
        dict: A dictionary containing the processed details of the fastest itinerary.
            The dictionary has the following keys:
            - "TotalDurationMin": The total duration of the itinerary in minutes.
            - "TripDistanceKm": The total distance of the itinerary in kilometers.
            - "TotalWalkingTimeMin": The total walking time of the itinerary in minutes.
            - "TotalTransitTimeMin": The total transit time of the itinerary in minutes.
            - "Changes": The number of mode changes in the itinerary.
            - "PickupStationProximity": The distance to the pickup station in meters, or 0 if not available.
            - "DropoffStationProximity": The distance to the dropoff station in meters, or 0 if not available.
    """
    try:
        # Extract itineraries and find the fastest one
        itineraries = response['plan']['itineraries']
        fastest_itinerary = min(itineraries, key=lambda x: x['duration'])
        
        # Use list comprehensions to calculate walking and transit times, and distances
        walking_legs = [leg for leg in fastest_itinerary['legs'] if leg['mode'] == "WALK"]
        transit_legs = [leg for leg in fastest_itinerary['legs'] if leg['mode'] != "WALK"]

        total_walking_time = sum(leg['duration'] for leg in walking_legs) / 60.0
        total_transit_time = sum(leg['duration'] for leg in transit_legs) / 60.0
        total_distance = sum(leg['distance'] for leg in fastest_itinerary['legs']) / 1000
        
        # Get the first walking leg's distance if it exists
        distance_to_station = walking_legs[0]['distance'] if walking_legs else np.nan
        # get the last walking leg's distance if it exists
        distance_to_destination = walking_legs[-1]['distance'] if walking_legs else np.nan

        # Calculate changes by comparing modes between subsequent legs (ignoring 'WALK' legs)
        modes = [leg['mode'] for leg in transit_legs]
        changes = sum(m1 != m2 for m1, m2 in zip(modes, modes[1:]))

        # Construct the details dictionary
        details = {
            "TotalDurationMin": fastest_itinerary['duration'] / 60.0,
            "TripDistanceKm": total_distance,
            "TotalWalkingTimeMin": total_walking_time,
            "TotalTransitTimeMin": total_transit_time,
            "Changes": changes,
            'PickupStationProximity': distance_to_station if not np.isnan(distance_to_station) else 0,
            "DropoffStationProximity": distance_to_destination if not np.isnan(distance_to_destination) else 0
        }
        return details
    except KeyError as e:
        print(f"Error processing itinerary: {e}")
        # Return NaN values if there's an error in the response format
        return {
            "TotalDurationMin": np.nan,
            "TripDistanceKm": np.nan,
            "TotalWalkingTimeMin": np.nan,
            "TotalTransitTimeMin": np.nan,
            "Changes": np.nan,
            'PickupStationProximity': np.nan,
            "DropoffStationProximity": np.nan
        }
    



# This function is a wrapper around the 'fetch_and_process_fastest_itinerary' for concurrent processing
def fetch_and_process_itinerary_concurrent(start_lat, start_lon, end_lat, end_lon, start_time, num_itineraries):
    response = get_top_itineraries(start_lat, start_lon, end_lat, end_lon, start_time, num_itineraries=num_itineraries)
    return fetch_and_process_fastest_itinerary(response)

# The main function to process the DataFrame concurrently and update the results
def main_concurrent(df, num_itineraries):
    """
    Process itineraries concurrently using a thread pool executor.

    Args:
        df (pandas.DataFrame): The input DataFrame containing itinerary data.
        num_itineraries (int): The number of itineraries to fetch and process for each row.

    Returns:
        None
    """
    # Temporary dictionary to hold the results
    results = {}
    with ThreadPoolExecutor(max_workers=50) as executor:
        # Prepare and submit all futures
        futures_to_index = {
            executor.submit(
                fetch_and_process_itinerary_concurrent,
                row['StartClusterLatitude'], row['StartClusterLongitude'],
                row['EndClusterLatitude'], row['EndClusterLongitude'],
                row['StartTimeUpdated'],
                num_itineraries
            ): index for index, row in df.iterrows()
        }
        
        # Process futures as they complete and show progress
        for future in tqdm(as_completed(futures_to_index), total=len(futures_to_index), desc="Processing itineraries"):
            index = futures_to_index[future]
            try:
                results[index] = future.result()
            except Exception as exc:
                print(f'Row {index} generated an exception: {exc}')
                results[index] = {
                    "TotalDurationMin": np.nan,
                    "TripDistanceKm": np.nan,
                    "TotalWalkingTimeMin": np.nan,
                    "TotalTransitTimeMin": np.nan,
                    "Changes": np.nan,
                    'PickupStationProximity': np.nan,
                    "DropoffStationProximity": np.nan
                }
    
    # Update the DataFrame outside the thread pool
    for index, data in results.items():
        for key, value in data.items():
            df.at[index, key] = value


# The main function to process the DataFrame concurrently and update the results
def main_concurrent_sn(df, num_itineraries):
    """
    Process itineraries concurrently using a thread pool executor.

    Args:
        df (pandas.DataFrame): The input DataFrame containing itinerary data.
        num_itineraries (int): The number of itineraries to fetch and process for each row.

    Returns:
        None
    """
    # Temporary dictionary to hold the results
    results = {}
    with ThreadPoolExecutor(max_workers=50) as executor:
        # Prepare and submit all futures
        futures_to_index = {
            executor.submit(
                fetch_and_process_itinerary_concurrent,
                row['LatitudeStart'], row['LongitudeStart'],
                row['LatitudeEnd'], row['LongitudeEnd'],
                row['StartTimeUpdated'],
                num_itineraries
            ): index for index, row in df.iterrows()
        }
        
        # Process futures as they complete and show progress
        for future in tqdm(as_completed(futures_to_index), total=len(futures_to_index), desc="Processing itineraries"):
            index = futures_to_index[future]
            try:
                results[index] = future.result()
            except Exception as exc:
                print(f'Row {index} generated an exception: {exc}')
                results[index] = {
                    "TotalDurationMin": np.nan,
                    "TripDistanceKm": np.nan,
                    "TotalWalkingTimeMin": np.nan,
                    "TotalTransitTimeMin": np.nan,
                    "Changes": np.nan,
                    'PickupStationProximity': np.nan,
                    "DropoffStationProximity": np.nan
                }
    
    # Update the DataFrame outside the thread pool
    for index, data in results.items():
        for key, value in data.items():
            df.at[index, key] = value