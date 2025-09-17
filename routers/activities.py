from services import update_service
from fastapi import APIRouter, Query
from typing import Optional, List
import pandas as pd
from services.activity_service import *
from datetime import datetime, timedelta

router = APIRouter()


@router.get("/filter_activities")
def filter_activities(
    sport_type: Optional[str] = Query(None, description="Filtrer par type de sport"),
    start_date: Optional[str] = Query(None, description="Filtrer les activités après cette date YYYY-MM-DD")
):
    """
    Renvoie toutes les activités filtrées.
    """
    df = get_all_activities()
    if df.empty:
        return {"activities": []}

    if sport_type:
        df = df[df["sport_type"] == sport_type]
    if start_date:
        df = df[df["start_date"] >= pd.to_datetime(start_date)]

    return {"activities": df.to_dict(orient="records")}

@router.get("/last_activity")
def last_activity(sport_type: Optional[str] = Query(None)):
    result = get_last_activity(sport_type=sport_type)
    if not result:
        return {"message": f"Aucune activité trouvée pour le sport '{sport_type}'."}
    return result

@router.get("/activities")
def all_activities():
    df = get_all_activities()
    return df.to_dict(orient="records")

@router.get("/last_activity_streams")
def last_activity_streams(sport_type: Optional[str] = Query(None)):
    return get_last_activity_streams(sport_type)



@router.get("/activity_streams")
def activity_streams(activity_id: str = Query(..., description="ID de l'activité Strava")):
    streams = get_streams_for_activity(activity_id)
    if not streams:
        return {"message": f"Aucune donnée de streams trouvée pour l'activité ID {activity_id}."}

    # Extraire les coordonnées pour le polyligne
    coords = [(point['lat'], point['lon']) for point in streams if point['lat'] is not None and point['lon'] is not None]

    return {
        "streams": streams
    }



@router.post("/update_db")
def update_db():
    """
    Met à jour la base de données avec les nouvelles activités Strava.
    """
    result = update_service.update_activities_database()
    return {"status": "ok", "message": result}

@router.post("/update_streams")
def update_streams():
    """
    Met à jour la base de données avec les nouveaux streams Strava.
    """
    result = update_service.update_streams_database()
    return {"status": "ok", "message": result}
