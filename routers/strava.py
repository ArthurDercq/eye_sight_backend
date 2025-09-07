from fastapi import APIRouter
from typing import Optional
from services import strava_service, db_service, update_service
from datetime import datetime

router = APIRouter()

@router.get("/athlete")
def athlete():
    header = strava_service.get_header()
    return {"message": "Athlete token ready", "header": header}

@router.post("/fetch_activities")
def fetch_activities(after_date: Optional[str] = None):
    """
    Récupère les activités Strava après une date optionnelle et les stocke dans PostgreSQL.
    after_date : format YYYY-MM-DD
    """
    dt = None
    if after_date:
        dt = datetime.strptime(after_date, "%Y-%m-%d")

    # Fetch des activités
    df = strava_service.fetch_activities(after_date=dt)

    # Stockage dans la base PostgreSQL
    db_service.store_df_in_postgresql(df)

    # Retour JSON pour le frontend
    return {"nb_activities": len(df), "activities": df.to_dict(orient="records")}

@router.post("/fetch_streams")
def fetch_streams(max_per_15min: int = 590):
    """
    Récupère les streams Strava pour toutes les activités déjà présentes dans la DB
    et les stocke dans PostgreSQL.
    """
    # Récupérer le header Strava
    header = strava_service.get_header()

    # Récupérer tous les activity_ids depuis PostgreSQL
    activity_ids = db_service.get_all_activity_ids_from_db()

    # Récupérer les streams
    df_streams = strava_service.fetch_multiple_streams(activity_ids, header, max_per_15min=max_per_15min)

    # Stocker dans PostgreSQL
    db_service.store_df_streams_in_postgresql(df_streams)

    return {"nb_streams": len(df_streams)}


@router.post("/update_activities")
def update_activities():
    result = update_service.update_activities_database()
    return {"message": result}

@router.post("/update_streams")
def update_streams():
    result = update_service.update_streams_database()
    return {"message": result}
