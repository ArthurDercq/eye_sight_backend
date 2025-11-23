"""
Service CRUD pour les activités.
Gère la création, mise à jour et suppression des activités avec impact sur la DB.
"""
import json
from datetime import datetime
from typing import Optional
from db.connection import get_conn
from models.activity import ActivityCreate, ActivityUpdate
import pandas as pd


def calculate_derived_fields(data: dict, force_recalc: bool = False) -> dict:
    """
    Calcule les champs dérivés pour une activité.
    - moving_time_hms et elapsed_time_hms
    - speed_minutes_per_km et speed_minutes_per_km_hms
    - average_speed si non fourni

    Args:
        data: Données de l'activité
        force_recalc: Si True, recalcule average_speed même s'il existe déjà
    """
    result = data.copy()

    # Calculer average_speed (toujours si force_recalc ou si non fourni)
    if result.get('distance') and result.get('moving_time'):
        if force_recalc or not result.get('average_speed'):
            # distance en km, moving_time en minutes -> km/h
            result['average_speed'] = (result['distance'] / result['moving_time']) * 60

    # Calculer moving_time_hms
    if result.get('moving_time'):
        total_seconds = int(result['moving_time'] * 60)
        h = total_seconds // 3600
        m = (total_seconds % 3600) // 60
        s = total_seconds % 60
        result['moving_time_hms'] = f"{h:02d}:{m:02d}:{s:02d}"

    # Calculer elapsed_time_hms
    if result.get('elapsed_time'):
        total_seconds = int(result['elapsed_time'] * 60)
        h = total_seconds // 3600
        m = (total_seconds % 3600) // 60
        s = total_seconds % 60
        result['elapsed_time_hms'] = f"{h:02d}:{m:02d}:{s:02d}"

    # Calculer speed_minutes_per_km
    if result.get('average_speed') and result['average_speed'] > 0:
        result['speed_minutes_per_km'] = 60 / result['average_speed']

        # Format mm:ss
        minutes = int(result['speed_minutes_per_km'])
        seconds = int((result['speed_minutes_per_km'] % 1) * 60)
        result['speed_minutes_per_km_hms'] = f"{minutes}:{seconds:02d}"
    else:
        result['speed_minutes_per_km'] = None
        result['speed_minutes_per_km_hms'] = None

    return result


def create_activity(activity: ActivityCreate) -> dict:
    """
    Crée une nouvelle activité dans la base de données.
    Retourne l'activité créée avec son ID.

    Raises:
        Exception: Si l'insertion échoue
    """
    # Convertir le modèle Pydantic en dict
    data = activity.model_dump()

    # Calculer les champs dérivés
    data = calculate_derived_fields(data)

    # Convertir les dates en string ISO
    if isinstance(data.get('start_date'), datetime):
        data['start_date'] = data['start_date'].isoformat()
    if isinstance(data.get('start_date_local'), datetime):
        data['start_date_local'] = data['start_date_local'].isoformat()

    # Convertir map en JSON (ou None si vide)
    if data.get('map'):
        if isinstance(data['map'], dict):
            # Convertir dict en JSON string, ou None si vide
            data['map'] = json.dumps(data['map']) if data['map'] else None
    else:
        data['map'] = None

    with get_conn() as conn:
        with conn.cursor() as cur:
            # Colonnes à insérer
            columns = [
                'name', 'distance', 'moving_time', 'elapsed_time', 'moving_time_hms',
                'elapsed_time_hms', 'average_speed', 'speed_minutes_per_km',
                'speed_minutes_per_km_hms', 'total_elevation_gain', 'sport_type',
                'start_date', 'start_date_local', 'timezone', 'achievement_count',
                'kudos_count', 'gear_id', 'start_latlng', 'end_latlng', 'max_speed',
                'average_cadence', 'average_temp', 'has_heartrate', 'average_heartrate',
                'max_heartrate', 'elev_high', 'elev_low', 'pr_count', 'has_kudoed',
                'average_watts', 'kilojoules', 'map'
            ]

            # Préparer les valeurs (seulement celles qui existent dans data)
            values = [data.get(col) for col in columns]
            placeholders = ', '.join(['%s'] * len(columns))
            columns_str = ', '.join(columns)

            query = f"""
                INSERT INTO activites ({columns_str})
                VALUES ({placeholders})
                RETURNING *;
            """

            cur.execute(query, values)
            result = cur.fetchone()
            conn.commit()

            # Convertir le résultat en dict
            if result:
                # Convertir map de JSON string à dict
                if result.get('map'):
                    result['map'] = json.loads(result['map']) if isinstance(result['map'], str) else result['map']

                # Convertir dates en ISO string
                if result.get('start_date'):
                    result['start_date'] = result['start_date'].isoformat()
                if result.get('start_date_local'):
                    result['start_date_local'] = result['start_date_local'].isoformat()

                return result

            raise Exception("Failed to create activity")


def update_activity(activity_id: int, activity: ActivityUpdate) -> Optional[dict]:
    """
    Met à jour une activité existante.
    Ne met à jour que les champs fournis (PATCH partiel).

    Args:
        activity_id: ID de l'activité à mettre à jour
        activity: Données à mettre à jour

    Returns:
        L'activité mise à jour ou None si non trouvée
    """
    # Convertir le modèle en dict et exclure les valeurs None
    data = activity.model_dump(exclude_none=True)

    if not data:
        # Aucune donnée à mettre à jour
        return get_activity_by_id(activity_id)

    # Recalculer les champs dérivés si les champs de base changent
    if any(k in data for k in ['distance', 'moving_time', 'elapsed_time', 'average_speed']):
        # Récupérer l'activité existante pour avoir les valeurs de base
        existing = get_activity_by_id(activity_id)
        if not existing:
            return None

        # Merger les données existantes avec les nouvelles
        merged = {**existing, **data}
        # Force le recalcul si distance ou moving_time ont changé
        force_recalc = 'distance' in data or 'moving_time' in data
        merged = calculate_derived_fields(merged, force_recalc=force_recalc)

        # Garder seulement les nouveaux champs + les dérivés recalculés
        derived_fields = ['moving_time_hms', 'elapsed_time_hms', 'speed_minutes_per_km', 'speed_minutes_per_km_hms', 'average_speed']
        for field in derived_fields:
            if field in merged:
                data[field] = merged[field]

    # Convertir les dates en string ISO
    if isinstance(data.get('start_date'), datetime):
        data['start_date'] = data['start_date'].isoformat()
    if isinstance(data.get('start_date_local'), datetime):
        data['start_date_local'] = data['start_date_local'].isoformat()

    # Convertir map en JSON (ou None si vide)
    if data.get('map'):
        if isinstance(data['map'], dict):
            # Convertir dict en JSON string, ou None si vide
            data['map'] = json.dumps(data['map']) if data['map'] else None
    else:
        data['map'] = None

    with get_conn() as conn:
        with conn.cursor() as cur:
            # Construire la requête UPDATE dynamiquement
            set_clause = ', '.join([f"{col} = %s" for col in data.keys()])
            values = list(data.values())
            values.append(activity_id)  # Pour le WHERE

            query = f"""
                UPDATE activites
                SET {set_clause}
                WHERE id = %s
                RETURNING *;
            """

            cur.execute(query, values)
            result = cur.fetchone()
            conn.commit()

            if result:
                # Convertir map de JSON string à dict
                if result.get('map'):
                    result['map'] = json.loads(result['map']) if isinstance(result['map'], str) else result['map']

                # Convertir dates en ISO string
                if result.get('start_date'):
                    result['start_date'] = result['start_date'].isoformat()
                if result.get('start_date_local'):
                    result['start_date_local'] = result['start_date_local'].isoformat()

                return result

            return None


def delete_activity(activity_id: int, delete_streams: bool = True) -> bool:
    """
    Supprime une activité de la base de données.

    Args:
        activity_id: ID de l'activité à supprimer
        delete_streams: Si True, supprime aussi les streams associés

    Returns:
        True si l'activité a été supprimée, False sinon
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Supprimer les streams associés si demandé
            if delete_streams:
                cur.execute("DELETE FROM streams WHERE activity_id = %s;", (str(activity_id),))

            # Supprimer l'activité
            cur.execute("DELETE FROM activites WHERE id = %s;", (activity_id,))
            deleted = cur.rowcount > 0

            conn.commit()
            return deleted


def get_activity_by_id(activity_id: int) -> Optional[dict]:
    """
    Récupère une activité par son ID.

    Returns:
        L'activité ou None si non trouvée
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM activites WHERE id = %s;", (activity_id,))
            result = cur.fetchone()

            if result:
                # Convertir map de JSON string à dict
                if result.get('map'):
                    result['map'] = json.loads(result['map']) if isinstance(result['map'], str) else result['map']

                # Convertir dates en ISO string
                if result.get('start_date'):
                    result['start_date'] = result['start_date'].isoformat()
                if result.get('start_date_local'):
                    result['start_date_local'] = result['start_date_local'].isoformat()

                return result

            return None


def activity_exists(activity_id: int) -> bool:
    """
    Vérifie si une activité existe.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT EXISTS(SELECT 1 FROM activites WHERE id = %s);", (activity_id,))
            result = cur.fetchone()
            return result['exists'] if result else False
