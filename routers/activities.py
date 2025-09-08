from services import update_service
from fastapi import APIRouter, Query
from typing import Optional, List
import pandas as pd
from services.activity_service import get_all_activities, prepare_weekly_data, prepare_monthly_data, prepare_daily_data, aggregate_distance, prepare_weekly_avg_speed_by_sport
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


@router.get("/aggregate")
def get_aggregated_activities(
    period: str = Query("W", description="Période d'agrégation: D=jour, W=semaine, M=mois, Y=année"),
    value_col: str = Query("moving_time", description="Colonne à agréger: moving_time ou distance"),
    sport_types: Optional[List[str]] = Query(None, description="Filtre sur le(s) type(s) de sport"),
    periods: int = Query(12, description="Nombre de périodes à afficher")
):
    """
    Renvoie les activités agrégées par période.
    """
    df = get_all_activities()
    if df.empty:
        return {"data": []}

    df_agg = aggregate_distance(df=df, period=period, sport_filter=sport_types)

    return {"aggregated": df_agg.to_dict(orient="records")}

@router.get("/weekly_avg_speed")
def weekly_avg_speed(weeks: int = 12, sport_types: Optional[list[str]] = None):
    df = get_all_activities()
    result = prepare_weekly_avg_speed_by_sport(df, weeks=weeks, sport_types=sport_types)
    return result.to_dict(orient="records")

@router.get("/hours_bar")
def hours_bar(value_col: str = "moving_time", weeks: int = 12):
    df = get_all_activities()
    weekly_df = prepare_weekly_data(df, value_col=value_col, weeks=weeks)
    return get_hours_bar_data(weekly_df, value_col=value_col)

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

@router.get("/repartition_run")
def repartition_run(sport_type: str):
    df = get_all_activities()
    return get_repartition_run_data(df, sport_type)




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
