"""
Router pour les analyses avancées
"""
from fastapi import APIRouter, Query
from services.analysis_service import calculate_rolling_hr_speed_correlation

router = APIRouter()


@router.get("/rolling_hr_speed_correlation/{activity_id}")
def get_rolling_hr_speed_correlation(
    activity_id: str,
    window_seconds: int = Query(180, description="Taille de la fenêtre glissante en secondes", ge=30, le=600)
):
    """
    Calcule la corrélation glissante entre fréquence cardiaque et vitesse.

    Args:
        activity_id: ID de l'activité Strava
        window_seconds: Taille de la fenêtre glissante (défaut: 180s = 3min)

    Returns:
        Données de corrélation glissante et points de rupture
    """
    result = calculate_rolling_hr_speed_correlation(activity_id, window_seconds)
    return result
