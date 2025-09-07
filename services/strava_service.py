# services/strava_service.py
import time
from strava import fetch_strava

def get_header():
    return fetch_strava.get_strava_header()

def fetch_activities(after_date=None, return_header=False):
    return fetch_strava.fetch_strava_data(after_date=after_date, return_header=return_header)

def get_all_activity_ids(db_uri, table_name):
    return fetch_strava.get_all_activity_ids_from_db(db_uri, table_name)

def fetch_stream(activity_id, header):
    return fetch_strava.fetch_stream(activity_id, header)

def fetch_multiple_streams(activity_ids, header, max_per_15min=590):
    return fetch_strava.fetch_multiple_streams_df(activity_ids, header, max_per_15min=max_per_15min)
