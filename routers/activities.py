from services import update_service
from fastapi import APIRouter, Query, HTTPException, status
from typing import Optional, List
import pandas as pd
from services.activity_service import *
from services.activity_crud import (
    create_activity,
    update_activity,
    delete_activity,
    get_activity_by_id,
    activity_exists
)
from models.activity import ActivityCreate, ActivityUpdate, ActivityResponse
from datetime import datetime, timedelta

router = APIRouter()


@router.get("/filter_activities")
def filter_activities(
    sport_type: Optional[str] = Query(None, description="Filtrer par type de sport"),
    start_date: Optional[str] = Query(None, description="Filtrer les activités après cette date YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="Filtrer les activités avant cette date YYYY-MM-DD")
):
    """
    Renvoie toutes les activités filtrées.
    """
    df = get_all_activities()
    if df.empty:
        return {"activities": []}

    if sport_type:
        df = df[df["sport_type"] == sport_type]

    # Convertir start_date en datetime une seule fois
    df["start_date"] = pd.to_datetime(df["start_date"])

    if start_date:
        start_date_dt = pd.to_datetime(start_date)
        df = df[df["start_date"] >= start_date_dt]

    if end_date:
        end_date_dt = pd.to_datetime(end_date)
        # Ajouter 1 jour pour inclure toute la journée de end_date
        end_date_dt = end_date_dt + pd.Timedelta(days=1)
        df = df[df["start_date"] < end_date_dt]

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


@router.get("/activity_detail/{activity_id}")
def activity_detail(activity_id: str):
    """
    Renvoie les détails complets d'une activité avec ses streams.
    Inclut: info globale + streams (lat, lon, altitude, distance_m, time_s, heartrate, cadence, velocity_smooth, temp, power, grade_smooth)
    """
    # Récupérer les infos générales de l'activité
    df = get_all_activities()
    activity_info = df[df["id"] == int(activity_id)]

    if activity_info.empty:
        return {"error": f"Activité {activity_id} introuvable"}

    # Convertir en dict
    activity = activity_info.iloc[0].to_dict()

    # Récupérer les streams
    streams = get_streams_for_activity(activity_id)

    return {
        "activity": activity,
        "streams": streams if streams else []
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


# ============== CRUD Operations ==============

@router.post("/activities", response_model=ActivityResponse, status_code=status.HTTP_201_CREATED)
def create_new_activity(activity: ActivityCreate):
    """
    Crée une nouvelle activité manuellement.

    Les champs obligatoires sont:
    - name: Nom de l'activité
    - sport_type: Type de sport (Run, Ride, Trail, Bike, etc.)
    - start_date: Date de début
    - distance: Distance en km
    - moving_time: Temps de mouvement en minutes

    Les champs dérivés (speed, pace, etc.) sont calculés automatiquement.
    """
    try:
        result = create_activity(activity)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erreur lors de la création de l'activité: {str(e)}"
        )


@router.get("/activities/{activity_id}", response_model=ActivityResponse)
def get_activity(activity_id: int):
    """
    Récupère une activité par son ID.
    """
    activity = get_activity_by_id(activity_id)
    if not activity:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Activité {activity_id} introuvable"
        )
    return activity


@router.put("/activities/{activity_id}", response_model=ActivityResponse)
def update_existing_activity(activity_id: int, activity: ActivityUpdate):
    """
    Met à jour une activité existante (PATCH partiel).

    Seuls les champs fournis seront mis à jour.
    Les champs dérivés (speed, pace, etc.) sont recalculés si nécessaire.

    **Important**: Cette modification peut impacter vos statistiques, KPIs et records.
    """
    # Vérifier que l'activité existe
    if not activity_exists(activity_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Activité {activity_id} introuvable"
        )

    try:
        result = update_activity(activity_id, activity)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Échec de la mise à jour"
            )
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erreur lors de la mise à jour: {str(e)}"
        )


@router.patch("/activities/{activity_id}", response_model=ActivityResponse)
def patch_activity(activity_id: int, activity: ActivityUpdate):
    """
    Alias pour PUT - Met à jour partiellement une activité.
    """
    return update_existing_activity(activity_id, activity)


@router.delete("/activities/{activity_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_existing_activity(activity_id: int, delete_streams: bool = Query(True, description="Supprimer aussi les streams associés")):
    """
    Supprime une activité de la base de données.

    Par défaut, supprime aussi les streams (traces GPS) associés.

    **⚠️ ATTENTION**: Cette action est irréversible et impacte:
    - Vos statistiques globales
    - Vos KPIs
    - Vos records personnels
    - Les données de streams (traces GPS) si delete_streams=True
    """
    # Vérifier que l'activité existe
    if not activity_exists(activity_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Activité {activity_id} introuvable"
        )

    try:
        success = delete_activity(activity_id, delete_streams=delete_streams)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Échec de la suppression"
            )
        return None  # 204 No Content
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erreur lors de la suppression: {str(e)}"
        )
