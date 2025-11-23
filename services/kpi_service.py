import pandas as pd
from services.activity_service import get_all_activities
from datetime import datetime, timedelta


SPORT_MAPPING = {
    "TrailRun": "Trail",
    "Run": "Run",
    "Ride": "Bike",
    "Swim": "Swim"
}

def prepare_kpis(start_date=None, end_date=None):
    """
    Calcule les KPIs globaux pour les activités de l'utilisateur.

    :param start_date: datetime ou str (YYYY-MM-DD), filtre la période
    :param end_date: datetime ou str (YYYY-MM-DD), filtre la période
    :return: dict avec les KPIs
    """
    df = get_all_activities()
    if df.empty:
        return {
            "total_km_run": None,
            "total_km_trail": None,
            "total_km_run_trail": None,
            "total_km_bike": None,
            "total_km_swim": None,
            "total_hours": None,
            "total_dplus_run": None,
            "total_dplus_trail": None,
            "total_dplus_run_trail": None,
            "total_dplus_bike": None
        }

    # Normaliser les dates et sports
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["sport_type"] = df["sport_type"].map(lambda s: SPORT_MAPPING.get(s, s))

    # Filtrage par période
    if start_date:
        start_date = pd.to_datetime(start_date)
        df = df[df["start_date"] >= start_date]
    if end_date:
        end_date = pd.to_datetime(end_date)
        # Add 1 day to include all activities on end_date (up to 23:59:59)
        end_date = end_date + pd.Timedelta(days=1)
        df = df[df["start_date"] < end_date]

    # Calcul des KPIs
    total_km_run = df[df["sport_type"] == "Run"]["distance"].sum()
    total_km_trail = df[df["sport_type"] == "Trail"]["distance"].sum()
    total_km_run_trail = total_km_run + total_km_trail
    total_km_bike = df[df["sport_type"] == "Bike"]["distance"].sum()
    total_km_swim = df[df["sport_type"] == "Swim"]["distance"].sum()

    total_dplus_run = df[df["sport_type"] == "Run"]["total_elevation_gain"].sum()
    total_dplus_trail = df[df["sport_type"] == "Trail"]["total_elevation_gain"].sum()
    total_dplus_run_trail = total_dplus_run + total_dplus_trail
    total_dplus_bike = df[df["sport_type"] == "Bike"]["total_elevation_gain"].sum()

    # Total heures de sport (elapsed_time en secondes → heures)
    total_hours = df["elapsed_time"].sum() / 60 if "elapsed_time" in df.columns else 0

    #Nombre d'activités par type de sport
    activity_counts = df["sport_type"].value_counts().to_dict()

    return {
        "total_km_run": round(total_km_run, 2),
        "total_km_trail": round(total_km_trail, 2),
        "total_km_run_trail": round(total_km_run_trail, 2),
        "total_km_bike": round(total_km_bike, 2),
        "total_km_swim": round(total_km_swim, 2),
        "total_hours": round(total_hours, 2),
        "total_dplus_run": round(total_dplus_run, 2),
        "total_dplus_trail": round(total_dplus_trail, 2),
        "total_dplus_run_trail": round(total_dplus_run_trail, 2),
        "total_dplus_bike": round(total_dplus_bike, 2),
        "nombre d'activités par sport": activity_counts
    }


def calculate_streak():
    """
    Calcule la série d'activités hebdomadaires consécutives.
    Conditions pour qu'une semaine compte:
    - Au moins 1 activité
    - Au moins 5 km courus (Run ou Trail)

    Retourne:
    - streak_weeks: nombre de semaines consécutives
    - total_activities: nombre total d'activités dans la streak
    """
    df = get_all_activities()
    if df.empty:
        return {"streak_weeks": 0, "total_activities": 0}

    # Normaliser les dates et sports
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["sport_type"] = df["sport_type"].map(lambda s: SPORT_MAPPING.get(s, s))

    # Trier par date décroissante (du plus récent au plus ancien)
    df = df.sort_values("start_date", ascending=False)

    # Calculer le numéro de semaine ISO (année, semaine)
    df["year"] = df["start_date"].dt.isocalendar().year
    df["week"] = df["start_date"].dt.isocalendar().week
    df["year_week"] = df["year"].astype(str) + "-W" + df["week"].astype(str).str.zfill(2)

    # Obtenir la semaine actuelle
    now = datetime.now()
    current_year = now.isocalendar().year
    current_week = now.isocalendar().week
    current_year_week = f"{current_year}-W{str(current_week).zfill(2)}"

    # Grouper par semaine
    weekly_groups = df.groupby("year_week").apply(
        lambda group: {
            "year": group["year"].iloc[0],
            "week": group["week"].iloc[0],
            "activity_count": len(group),
            "run_trail_distance": group[group["sport_type"].isin(["Run", "Trail"])]["distance"].sum(),
            "activities": group.index.tolist()
        },
        include_groups=False
    ).to_dict()

    # Générer la liste de toutes les semaines depuis la plus récente
    streak_weeks = 0
    total_activities = 0

    # Commencer à partir de la semaine actuelle
    check_year = current_year
    check_week = current_week

    while True:
        year_week_key = f"{check_year}-W{str(check_week).zfill(2)}"

        if year_week_key in weekly_groups:
            week_data = weekly_groups[year_week_key]
            # Vérifier les conditions: au moins 1 activité ET au moins 5 km Run/Trail
            if week_data["activity_count"] >= 1 and week_data["run_trail_distance"] >= 5:
                streak_weeks += 1
                total_activities += week_data["activity_count"]
            else:
                # La semaine ne valide pas les conditions, on arrête la streak
                break
        else:
            # Pas d'activité cette semaine, on arrête la streak
            break

        # Passer à la semaine précédente
        check_week -= 1
        if check_week < 1:
            # Passer à l'année précédente
            check_year -= 1
            # Obtenir le nombre de semaines dans l'année précédente
            last_day_of_year = datetime(check_year, 12, 31)
            check_week = last_day_of_year.isocalendar().week

    return {
        "streak_weeks": streak_weeks,
        "total_activities": total_activities
    }


def calculate_records():
    """
    Calcule les records de l'utilisateur sur les distances EXACTES standards.

    Pour chaque distance cible (5km, 10km, etc.), analyse TOUTES les activités Run/Trail
    et trouve le meilleur segment de cette distance exacte, même si l'activité est plus longue.

    Utilise les streams (données GPS) pour découper l'activité en segments et trouver
    le segment le plus rapide pour chaque distance.

    Distances analysées:
    - 5 km exact
    - 10 km exact
    - Semi-marathon (21.0975 km exact)
    - 30 km exact
    - Marathon (42.195 km exact)

    Retourne:
    {
        "5k": {"time": "00:23:45", "pace": "4:45", "date": "2024-01-15", "activity_id": "123", "start_km": 2.5, "end_km": 7.5},
        "10k": {...},
        ...
    }
    """
    from services.activity_service import get_streams_for_activity

    df = get_all_activities()
    if df.empty:
        return {
            "5k": None,
            "10k": None,
            "semi": None,
            "30k": None,
            "marathon": None
        }

    # Normaliser les sports
    df["sport_type"] = df["sport_type"].map(lambda s: SPORT_MAPPING.get(s, s))

    # Filtrer uniquement Run et Trail avec distance >= à la plus petite cible
    df = df[df["sport_type"].isin(["Run", "Trail"])].copy()
    df = df[df["distance"] >= 5.0].copy()  # Au moins 5km pour chercher des segments

    if df.empty:
        return {
            "5k": None,
            "10k": None,
            "semi": None,
            "30k": None,
            "marathon": None
        }

    # Distances cibles EXACTES (en km)
    target_distances = {
        "5k": 5.0,
        "10k": 10.0,
        "semi": 21.0975,
        "30k": 30.0,
        "marathon": 42.195
    }

    records = {}

    # Optimisation: trier par date décroissante et limiter aux 100 activités les plus récentes
    # (les records sont généralement dans les activités récentes)
    df = df.sort_values('start_date', ascending=False).head(100)

    for record_key, target_km in target_distances.items():
        target_meters = target_km * 1000  # Convertir en mètres
        best_record = None

        # Filtrer les activités assez longues pour cette distance
        candidates = df[df["distance"] >= target_km].copy()

        # Optimisation: trier par vitesse moyenne (les plus rapides en premier)
        # Cela nous permet de trouver rapidement de bons candidats
        candidates['avg_speed'] = candidates['distance'] / (candidates['moving_time'] / 60)  # km/h
        candidates = candidates.sort_values('avg_speed', ascending=False)

        # Limiter à 20 meilleures activités par distance pour accélérer
        candidates = candidates.head(20)

        # Parcourir les meilleures activités
        for _, activity in candidates.iterrows():
            activity_id = str(activity["id"])

            # Récupérer les streams
            try:
                streams = get_streams_for_activity(activity_id)
                if not streams or len(streams) == 0:
                    continue

                streams_df = pd.DataFrame(streams)

                # Vérifier qu'on a les colonnes nécessaires
                if 'distance_m' not in streams_df.columns or 'time_s' not in streams_df.columns:
                    continue

                # Nettoyer et trier
                streams_df = streams_df.dropna(subset=['distance_m', 'time_s'])
                streams_df = streams_df.sort_values('time_s').reset_index(drop=True)

                if len(streams_df) < 2:
                    continue

                # Trouver le meilleur segment de distance exacte
                best_segment = find_best_segment(streams_df, target_meters)

                if best_segment is None:
                    continue

                # Comparer avec le record actuel
                if best_record is None or best_segment['duration'] < best_record['duration']:
                    best_record = {
                        'duration': best_segment['duration'],
                        'start_distance_km': best_segment['start_distance_km'],
                        'end_distance_km': best_segment['end_distance_km'],
                        'activity_id': activity_id,
                        'activity_name': activity.get('name', ''),
                        'activity_date': pd.to_datetime(activity['start_date']).strftime("%Y-%m-%d")
                    }

            except Exception as e:
                # Si erreur sur une activité, continuer avec les autres
                continue

        # Formatter le record trouvé
        if best_record is None:
            records[record_key] = None
        else:
            duration_seconds = int(best_record['duration'])
            hours = duration_seconds // 3600
            minutes = (duration_seconds % 3600) // 60
            seconds = duration_seconds % 60

            if hours > 0:
                time_formatted = f"{hours}:{minutes:02d}:{seconds:02d}"
            else:
                time_formatted = f"{minutes}:{seconds:02d}"

            # Calculer l'allure (min/km)
            pace_seconds_per_km = duration_seconds / target_km
            pace_minutes = int(pace_seconds_per_km // 60)
            pace_seconds = int(pace_seconds_per_km % 60)
            pace_formatted = f"{pace_minutes}:{pace_seconds:02d}"

            records[record_key] = {
                "time": time_formatted,
                "pace": pace_formatted,
                "date": best_record['activity_date'],
                "activity_id": best_record['activity_id'],
                "activity_name": best_record['activity_name'],
                "activity_url": f"https://www.strava.com/activities/{best_record['activity_id']}",
                "distance": target_km,
                "start_km": round(best_record['start_distance_km'], 2),
                "end_km": round(best_record['end_distance_km'], 2)
            }

    return records


def find_best_segment(streams_df, target_meters):
    """
    Trouve le segment le plus rapide d'une distance exacte dans un stream d'activité.

    Utilise une fenêtre glissante OPTIMISÉE pour parcourir les segments possibles
    et retourne celui avec le temps le plus court.

    Args:
        streams_df: DataFrame avec colonnes 'distance_m' et 'time_s'
        target_meters: Distance cible en mètres (ex: 5000 pour 5km)

    Returns:
        dict avec 'duration', 'start_distance_km', 'end_distance_km' ou None
    """
    import bisect

    if len(streams_df) < 2:
        return None

    distances = streams_df['distance_m'].values
    times = streams_df['time_s'].values

    best_duration = None
    best_start_idx = None
    best_end_idx = None

    tolerance = 50  # Tolérance de 50m

    # Optimisation: échantillonner les points de départ tous les 100m au lieu de chaque point
    # Cela réduit drastiquement le nombre d'itérations tout en gardant une bonne précision
    sample_interval = max(1, len(distances) // 500)  # Au plus 500 échantillons

    # Fenêtre glissante optimisée
    for start_idx in range(0, len(distances), sample_interval):
        start_distance = distances[start_idx]
        start_time = times[start_idx]
        target_end_distance = start_distance + target_meters

        # Recherche binaire pour trouver l'index de fin
        end_idx = bisect.bisect_left(distances, target_end_distance, lo=start_idx + 1)

        if end_idx >= len(distances):
            break  # Plus assez de distance restante

        # Vérifier si on est proche de la distance cible
        actual_distance = distances[end_idx] - start_distance
        if abs(actual_distance - target_meters) > tolerance:
            continue

        # Calculer la durée de ce segment
        duration = times[end_idx] - start_time

        # Garder le meilleur
        if best_duration is None or duration < best_duration:
            best_duration = duration
            best_start_idx = start_idx
            best_end_idx = end_idx

    if best_duration is None:
        return None

    return {
        'duration': best_duration,
        'start_distance_km': distances[best_start_idx] / 1000,
        'end_distance_km': distances[best_end_idx] / 1000
    }
