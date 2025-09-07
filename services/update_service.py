# services/update_service.py
from strava.fetch_strava import fetch_strava_data, get_strava_header, fetch_multiple_streams_df
from strava.clean_data import clean_data
from strava.store_data import store_df_in_postgresql, store_df_streams_in_postgresql
from strava.params import *
from sqlalchemy import create_engine, text

from services.db_service import get_engine  # si tu veux utiliser le moteur SQLAlchemy du backend


def get_last_activity_date():
    """
    Récupère la date de la dernière activité dans la table activites
    """
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT MAX(start_date) FROM {TABLE_NAME}"))
        last_date = result.scalar()
    return last_date


def update_strava():
    """
    Récupère les nouvelles activités depuis Strava après la dernière date
    """
    last_date = get_last_activity_date()
    new_data = fetch_strava_data(after_date=last_date)
    if new_data.empty:
        return None
    return new_data


def update_activities_database():
    """
    Met à jour la table activites avec les nouvelles activités Strava
    """
    new_data = update_strava()
    if new_data is None:
        return "Aucune nouvelle activité trouvée"

    cleaned_data = clean_data(new_data)
    store_df_in_postgresql(
        cleaned_data,
        host=HOST,
        database=DATABASE,
        user=USER,
        password=PASSWORD,
        port=PORT
    )

    return f"{len(cleaned_data)} nouvelle(s) activité(s) ajoutée(s)"


def update_streams_database():
    """
    Met à jour la table streams avec les nouvelles activités
    """
    new_data = update_strava()
    if new_data is None:
        return "Aucune nouvelle activité trouvée"

    activity_ids = new_data["id"].tolist()
    if not activity_ids:
        return "Aucun nouvel ID d'activité"

    header = get_strava_header()
    streams_df = fetch_multiple_streams_df(activity_ids, header)

    if streams_df.empty:
        return "Aucun stream récupéré"

    store_df_streams_in_postgresql(
        streams_df,
        host=HOST,
        database=DATABASE,
        user=USER,
        password=PASSWORD,
        port=PORT
    )

    return f"{len(streams_df)} stream(s) ajouté(s)"
