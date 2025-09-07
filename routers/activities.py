from fastapi import APIRouter
from services import update_service


router = APIRouter()

@router.get("/")
def get_activities():
    # temporaire, à remplacer par ton service DB
    return {"activities": []}

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
