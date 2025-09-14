from services import update_service
from fastapi import APIRouter, Query
from typing import Optional, List
import pandas as pd
from services.activity_service import *
from services.plot_service import get_hours_bar_data, get_weekly_intensity_data, get_calendar_heatmap_data, get_repartition_run_data
from datetime import datetime, timedelta

router = APIRouter()


@router.get("/")
def list_activities(
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
