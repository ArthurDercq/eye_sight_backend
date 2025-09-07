import os
from dotenv import load_dotenv

load_dotenv()

### CONSTANT STRAVA TOKEN ###
AUTH_URL = os.getenv('AUTH_URL')
ACTIVITES_URL = os.getenv('ACTIVITES_URL')

STRAVA_CLIENT_ID = os.getenv('STRAVA_CLIENT_ID')
STRAVA_CLIENT_SECRET = os.getenv('STRAVA_CLIENT_SECRET')
STRAVA_REFRESH_TOKEN = os.getenv('STRAVA_REFRESH_TOKEN')



### DATABSE ###
DB_PATH = os.getenv("DB_PATH")


## Postegresql ##

HOST = os.getenv("HOST")
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
PORT = os.getenv("PORT")

TABLE_NAME = os.getenv("TABLE_NAME")
TABLE_NAME2 = os.getenv("TABLE_NAME2")


DB_URI = os.getenv("DB_URI")
