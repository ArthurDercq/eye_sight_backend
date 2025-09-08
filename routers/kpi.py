from fastapi import APIRouter, Query
from typing import Optional
from services.kpi_service import prepare_kpis, get_last_activity

router = APIRouter()

@router.get("/")
def get_kpis(
    start_date: Optional[str] = Query(None, description="Date de début YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="Date de fin YYYY-MM-DD")
):
    """
    Renvoie les KPIs globaux pour les activités de l'utilisateur.
    """
    kpis = prepare_kpis(start_date=start_date, end_date=end_date)
    return {"kpis": kpis}

@router.get("/last_activity")
def last_activity(sport_type: Optional[str] = Query(None)):
    result = get_last_activity(sport_type=sport_type)
    if not result:
        return {"message": f"Aucune activité trouvée pour le sport '{sport_type}'."}
    return result
