import os
import pandas as pd
from math import cos, asin, sqrt
from timeit import default_timer as timer
from pathlib import Path
from datetime import datetime
import glob

def main():
    '''
    Refactor 
    '''
    repo_path = Path().parent.resolve()
    FILE_AIRPORTS  = f'{repo_path}/Input/world-airports.csv'
    FILE_ZIPCODES  = f'{repo_path}/Input/us_postal_codes.csv'
    zipcodes = pd.read_csv(FILE_ZIPCODES,encoding = "ISO-8859-1")
    airports = pd.read_csv(FILE_AIRPORTS, encoding = "ISO-8859-1")
    us_airports = prepare_airports_df(airports)
    states_scope = us_airports['iso_state'].unique()
    ## Remove extraneous stusab
    states_scope = [x for x in states_scope if x != 'U']
    perf_time = []
    entries = 0
    i = datetime.now()
    timestamp = i.strftime('%Y-%m%d-')
    try:
        start = timer()
        print("Calculating for the following states: ")
        [print (x) for x in states_scope]
        for state in states_scope:
            start_state = timer()
            calculateNearestAirport(
                state, 
                timestamp, 
                zipcodes, 
                us_airports,
                entries,
                repo_path)
            end_state = timer()
            diff = (end_state-start_state)
            time_state={
            'state': state,
            'duration': round(diff/60,3)
            }
            perf_time.append(time_state)

    finally:
        end = timer()
        print(round((end - start)/60,3), "minutes")
        print(len(perf_time), "states")
        print(entries, "zipcodes")
        for k in perf_time:
            print(k)
        npaths = [x for x in glob.glob(f'{repo_path}/Output/*_nearest_*.csv')]
        ndfs = [pd.read_csv(n) for n in npaths]
        pd.concat(ndfs).to_csv(f'{repo_path}/Output/us_nearest_airports.csv', index=False)
        print('Output full csv. Done!')

def prepare_airports_df(airports):
    columns_to_drop = ['elevation_ft', 'scheduled_service', 'gps_code',
       'home_link', 'wikipedia_link', 'keywords', 'score',
       'last_updated']
    airports.drop(columns_to_drop, axis=1, inplace=True)
    # filter only US and for only large airports
    us_airports = airports[(airports['iso_country']=='US'
        ) & (airports['type'].isin([
            'large_airport',
            #'medium_airport',
            #'small_airport'
            ]))]
    # Retrieve State Abbreviation
    us_airports.loc[:,'iso_state'] = us_airports['iso_region'].str.split('-').str[1]
    return us_airports


def get_airports(state, airport_df):
    df = airport_df[airport_df['iso_state']==state]
    return df.to_dict('records')

def get_zipcodes(state, df_zipcodes):
    df = df_zipcodes[(df_zipcodes['State Abbreviation']==state)]
    return df.to_dict('records')

def get_info(state):
    print(len(get_airports(state)), "airports in", state)
    print(len(get_zipcodes(state)), "zipcode in", state)


def distance(lat1, lon1, lat2, lon2):
    '''
    Function: distance, Purpose: Calculation
    Calculates distance between two points: zipcode lat-lon and airport lat-lon
    Based on Haversine Formula (found in StackOverflow)
    Uses math library
    '''
    p = 0.017453292519943295  #Pi/180
    a = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p)*cos(lat2*p) * (1-cos((lon2-lon1)*p)) / 2
    return 12742 * asin(sqrt(a)) #2*R*asin..

def closest(data, zipcode, repo_path):
    '''
    Function: closest
    Purpose: Calculation
    Runs distance function to given airport dataset
    Returns airport data with smallest distance to the given zipcode
    '''
    dl = []
    for p in data:
        ap = {
        'zipcode': zipcode['Zip Code'],
        'country': zipcode['Country'],
        'state': zipcode['State Abbreviation'],
        'state_full': zipcode['State'],
        'county': zipcode['County'],
        'latitude-zip': zipcode['Latitude'],
        'longitude-zip': zipcode['Longitude'],
        'nearest-airport': p['ident'],
        'latitude-air': p['latitude_deg'],
        'longitude-air': p['longitude_deg'],
        'distance': distance(zipcode['Latitude'],zipcode['Longitude'],p['latitude_deg'],p['longitude_deg'])
        }
        dl.append(ap)
    dl_sorted = sorted(dl, key=lambda k: k['distance'])
    zips_to_csv(dl_sorted, zipcode['State Abbreviation'], zipcode['Zip Code'], repo_path)
    return dl_sorted[0]

def zips_to_csv(dl_sorted, state, zipcode, repo_path):
    output_folder = f'{repo_path}/Output/{state}/'
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    pd.DataFrame(dl_sorted).to_csv(f'{output_folder}{str(zipcode)}_all airports.csv', index=False)


def calculateNearestAirport(
        state, 
        timestamp, 
        df_zipcodes,
        df_airports,
        entries, 
        repo_path
    ):
    try:
        zipcodes = get_zipcodes(state, df_zipcodes)
        dicts = []
        print(f'Calculating for {state} with {len(zipcodes)} zipcodes...')
        for zc in zipcodes:
            dicts.append(closest(get_airports(state, df_airports), zc, repo_path))
        pd.DataFrame(dicts).to_csv(f'{repo_path}/Output/{timestamp}{state}_nearest_airport.csv', index=False)
    finally:
        entries = entries + len(zipcodes)
        print(f'Done calculating for {len(zipcodes)} zipcodes of state')

if __name__ == "__main__":
    main()