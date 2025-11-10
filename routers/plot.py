import pandas as pd
from services.activity_service import get_all_activities
from fastapi import APIRouter, Query
from typing import Optional, List
from services.activity_service import *
from services.plot_service import *

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



@router.get("/calendar_heatmap")
def calendar_heatmap(value_col: str = "distance"):
    df = get_all_activities()
    return get_calendar_heatmap_data(df, value_col=value_col)


@router.get("/daily_hours_bar")
def daily_hours_bar(week_offset: int = Query(0, ge=0, le=52)):
    # Récupérer suffisamment de semaines pour couvrir l'offset demandé
    weeks_to_fetch = week_offset + 1
    df = get_recent_activities(weeks=weeks_to_fetch)
    return get_weekly_daily_barchart(df, week_offset)


@router.get("/poster_dplus")
def poster_dplus(
    n: int = Query(40, description="Nombre d'activités à récupérer"),
    sport_type: List[str] = Query(None, description="Types de sport à filtrer, ex: Trail,Run")
):
    data = get_poster_elev_profile(n=n, sport_type=sport_type)
    if not data:
        return {"message": "Aucune donnée de streams trouvée."}

    return {"poster_dplus": data}
