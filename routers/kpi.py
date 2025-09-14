from fastapi import APIRouter, Query
from typing import Optional
from services.kpi_service import prepare_kpis

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
