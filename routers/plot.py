import pandas as pd
from services.activity_service import get_all_activities
from fastapi import APIRouter, Query
from typing import Optional, List
from services.activity_service import *
from services.plot_service import get_hours_bar_data, get_weekly_intensity_data, get_calendar_heatmap_data, get_repartition_run_data
from datetime import datetime

router = APIRouter()



@router.get("/weekly_bar")
def weekly_bar(value_col: str = Query("moving_time", enum=["moving_time", "distance", "total_elevation_gain", "average_speed"]),
    weeks: int = Query(12, ge=1, le=52),
    sport_types: Optional[List[str]] = Query(None),
    year: Optional[int] = Query(None)):
    """
    Retourne un JSON agrégé par semaine pour les dernières `weeks`.
    """
    df = get_recent_activities(weeks=weeks)
    # Filtrage par sport_type si fourni
    if sport_types:
        df = df[df["sport_type"].isin(sport_types)]

    # Filtrage par année si fourni
    if year:
        df = df[df["start_date"].dt.year == year]

    weekly_df = aggregate_weekly(df, value_col=value_col)
    return weekly_df.to_dict(orient="records")


@router.get("/repartition_run")
def repartition_run(
    sport_type: Optional[List[str]] = Query(
        None, description="Nom du sport ou sports séparés par une virgule"
    ),
    weeks: int = Query(12, ge=1, le=52)
):
    df = get_recent_activities(weeks=weeks)

    # Si aucun sport n'est passé, utiliser les valeurs par défaut
    if not sport_type:
        sport_type = ["Run"]

    return get_repartition_run_data(df, sport_type)




@router.get("/weekly_intensity")
def weekly_intensity(week_start: str, week_end: str):
    df = get_all_activities()
    start = datetime.fromisoformat(week_start)
    end = datetime.fromisoformat(week_end)
    return get_weekly_intensity_data(df, start, end)

@router.get("/calendar_heatmap")
def calendar_heatmap(value_col: str = "distance"):
    df = get_all_activities()
    return get_calendar_heatmap_data(df, value_col=value_col)
