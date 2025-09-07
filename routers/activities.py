from fastapi import APIRouter

router = APIRouter()

@router.get("/")
def get_activities():
    # temporaire, à remplacer par ton service DB
    return {"activities": []}
