from fastapi import APIRouter, Query
from typing import Optional
from services.kpi_service import prepare_kpis, calculate_streak
from services.records_service import get_records_from_db, ensure_records_initialized

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


@router.get("/streak")
def get_streak():
    """
    Calcule la série d'activités hebdomadaires consécutives.
    Conditions: au moins 1 activité ET au moins 5 km Run/Trail par semaine.
    """
    streak_data = calculate_streak()
    return streak_data


@router.get("/records")
def get_records():
    """
    Retourne les records personnels de l'utilisateur depuis la base de données.

    Les records sont calculés une seule fois à l'initialisation puis mis à jour
    automatiquement quand une nouvelle activité bat un record.

    Distances analysées:
    - 5 km exact
    - 10 km exact
    - Semi-marathon (21.0975 km exact)
    - 30 km exact
    - Marathon (42.195 km exact)

    Pour chaque distance, retourne le meilleur segment trouvé dans toutes les activités.
    """
    # Vérifier si les records sont initialisés, sinon les initialiser
    ensure_records_initialized()

    # Récupérer depuis la DB (ultra rapide)
    records = get_records_from_db()
    return {"records": records}
