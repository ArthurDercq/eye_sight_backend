from pydantic import BaseModel, Field, validator
from typing import Optional
from datetime import datetime
import json


class ActivityCreate(BaseModel):
    """
    Modèle pour créer une nouvelle activité.
    Champs obligatoires minimum pour une activité valide.
    """
    name: str = Field(..., description="Nom de l'activité")
    sport_type: str = Field(..., description="Type de sport (Run, Ride, Trail, etc.)")
    start_date: datetime = Field(..., description="Date de début de l'activité")
    distance: float = Field(..., ge=0, description="Distance en km")
    moving_time: float = Field(..., ge=0, description="Temps de mouvement en minutes")

    # Champs optionnels avec valeurs par défaut
    elapsed_time: Optional[float] = Field(None, ge=0, description="Temps écoulé en minutes")
    total_elevation_gain: Optional[float] = Field(0, ge=0, description="Dénivelé en mètres")
    average_speed: Optional[float] = Field(None, ge=0, description="Vitesse moyenne en km/h")
    max_speed: Optional[float] = Field(None, ge=0, description="Vitesse max en km/h")
    average_heartrate: Optional[float] = Field(None, ge=0, description="Fréquence cardiaque moyenne")
    max_heartrate: Optional[float] = Field(None, ge=0, description="Fréquence cardiaque max")
    average_cadence: Optional[float] = Field(None, ge=0, description="Cadence moyenne")
    average_watts: Optional[float] = Field(None, ge=0, description="Puissance moyenne")
    kilojoules: Optional[float] = Field(None, ge=0, description="Énergie en kJ")
    average_temp: Optional[float] = Field(None, description="Température moyenne")
    device_watts: Optional[bool] = Field(None, description="Puissance mesurée par capteur")
    max_watts: Optional[int] = Field(None, ge=0, description="Puissance maximale en watts")
    weighted_average_watts: Optional[int] = Field(None, ge=0, description="Puissance moyenne pondérée")
    total_photo_count: Optional[int] = Field(0, ge=0, description="Nombre de photos")
    suffer_score: Optional[int] = Field(None, ge=0, description="Score de souffrance Strava")

    # Métadonnées optionnelles
    start_date_local: Optional[datetime] = None
    timezone: Optional[str] = None
    gear_id: Optional[str] = None
    start_latlng: Optional[str] = None
    end_latlng: Optional[str] = None

    # Compteurs sociaux
    achievement_count: Optional[int] = 0
    kudos_count: Optional[int] = 0
    pr_count: Optional[int] = 0

    # Flags
    has_heartrate: Optional[bool] = False
    has_kudoed: Optional[bool] = False

    # Élévations
    elev_high: Optional[float] = None
    elev_low: Optional[float] = None

    # Map polyline
    map: Optional[dict] = Field(default_factory=dict, description="Données de carte (polyline)")

    @validator('start_date_local', pre=True, always=True)
    def set_start_date_local(cls, v, values):
        """Si start_date_local n'est pas fourni, utiliser start_date"""
        return v or values.get('start_date')

    @validator('elapsed_time', pre=True, always=True)
    def set_elapsed_time(cls, v, values):
        """Si elapsed_time n'est pas fourni, utiliser moving_time"""
        return v or values.get('moving_time')

    class Config:
        json_schema_extra = {
            "example": {
                "name": "Morning Run",
                "sport_type": "Run",
                "start_date": "2025-11-23T08:00:00",
                "distance": 10.5,
                "moving_time": 55.3,
                "total_elevation_gain": 150,
                "average_speed": 11.4,
                "average_heartrate": 155
            }
        }


class ActivityUpdate(BaseModel):
    """
    Modèle pour mettre à jour une activité existante.
    Tous les champs sont optionnels.
    """
    name: Optional[str] = None
    sport_type: Optional[str] = None
    start_date: Optional[datetime] = None
    distance: Optional[float] = Field(None, ge=0)
    moving_time: Optional[float] = Field(None, ge=0)
    elapsed_time: Optional[float] = Field(None, ge=0)
    total_elevation_gain: Optional[float] = Field(None, ge=0)
    average_speed: Optional[float] = Field(None, ge=0)
    max_speed: Optional[float] = Field(None, ge=0)
    average_heartrate: Optional[float] = Field(None, ge=0)
    max_heartrate: Optional[float] = Field(None, ge=0)
    average_cadence: Optional[float] = Field(None, ge=0)
    average_watts: Optional[float] = Field(None, ge=0)
    kilojoules: Optional[float] = Field(None, ge=0)
    average_temp: Optional[float] = None
    device_watts: Optional[bool] = None
    max_watts: Optional[int] = Field(None, ge=0)
    weighted_average_watts: Optional[int] = Field(None, ge=0)
    total_photo_count: Optional[int] = None
    suffer_score: Optional[int] = None
    start_date_local: Optional[datetime] = None
    timezone: Optional[str] = None
    gear_id: Optional[str] = None
    start_latlng: Optional[str] = None
    end_latlng: Optional[str] = None
    achievement_count: Optional[int] = None
    kudos_count: Optional[int] = None
    pr_count: Optional[int] = None
    has_heartrate: Optional[bool] = None
    has_kudoed: Optional[bool] = None
    elev_high: Optional[float] = None
    elev_low: Optional[float] = None
    map: Optional[dict] = None

    class Config:
        json_schema_extra = {
            "example": {
                "name": "Updated Morning Run",
                "distance": 11.0,
                "moving_time": 57.0
            }
        }


class ActivityResponse(BaseModel):
    """
    Modèle de réponse pour une activité complète.
    Inclut tous les champs calculés.
    """
    id: int
    name: str
    sport_type: str
    start_date: str
    distance: float
    moving_time: float
    elapsed_time: Optional[float] = None
    moving_time_hms: Optional[str] = None
    elapsed_time_hms: Optional[str] = None
    total_elevation_gain: Optional[float] = None
    average_speed: Optional[float] = None
    speed_minutes_per_km: Optional[float] = None
    speed_minutes_per_km_hms: Optional[str] = None
    max_speed: Optional[float] = None
    average_heartrate: Optional[float] = None
    max_heartrate: Optional[float] = None
    average_cadence: Optional[float] = None
    average_watts: Optional[float] = None
    kilojoules: Optional[float] = None
    average_temp: Optional[float] = None
    device_watts: Optional[bool] = None
    max_watts: Optional[int] = None
    weighted_average_watts: Optional[int] = None
    total_photo_count: Optional[int] = None
    suffer_score: Optional[int] = None
    start_date_local: Optional[str] = None
    timezone: Optional[str] = None
    gear_id: Optional[str] = None
    start_latlng: Optional[str] = None
    end_latlng: Optional[str] = None
    achievement_count: Optional[int] = None
    kudos_count: Optional[int] = None
    pr_count: Optional[int] = None
    has_heartrate: Optional[bool] = None
    has_kudoed: Optional[bool] = None
    elev_high: Optional[float] = None
    elev_low: Optional[float] = None
    map: Optional[dict] = None

    class Config:
        from_attributes = True
