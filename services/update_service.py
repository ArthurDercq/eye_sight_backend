from strava.fetch_strava import fetch_strava_data, get_strava_header, fetch_multiple_streams_df
from strava.clean_data import clean_data
from strava.store_data import store_df_in_postgresql, store_df_streams_in_postgresql_optimized
from strava.params import *
from sqlalchemy import text
from db.connection import get_engine


def get_last_activity_date():
    """
    Récupère la date de la dernière activité dans la table activites
    """
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT MAX(start_date) FROM {TABLE_NAME}"))
        last_date = result.scalar()
    return last_date


def update_strava():
    """
    Récupère les nouvelles activités depuis Strava après la dernière date
    """
    last_date = get_last_activity_date()
    new_data = fetch_strava_data(after_date=last_date)
    if new_data.empty:
        return None
    return new_data


def update_activities_database():
    """
    Met à jour la table activites avec les nouvelles activités Strava
    """
    new_data = update_strava()
    if new_data is None:
        return "Aucune nouvelle activité trouvée"

    cleaned_data = clean_data(new_data)
    store_df_in_postgresql(
        cleaned_data,
        host=HOST,
        database=DATABASE,
        user=USER,
        password=PASSWORD,
        port=PORT
    )

    return f"{len(cleaned_data)} nouvelle(s) activité(s) ajoutée(s)"


def get_activities_without_streams(limit=None, recent_first=True):
    """
    Récupère les IDs des activités qui n'ont pas encore de streams

    Args:
        limit: Nombre max d'activités à récupérer (None = toutes)
        recent_first: Si True, récupère les plus récentes d'abord
    """
    engine = get_engine()
    with engine.connect() as conn:
        order_clause = "ORDER BY a.start_date DESC" if recent_first else ""
        limit_clause = f"LIMIT {limit}" if limit else ""

        query = f"""
            SELECT a.id
            FROM activites a
            LEFT JOIN streams s ON a.id::text = s.activity_id
            WHERE s.activity_id IS NULL
            {order_clause}
            {limit_clause}
        """
        result = conn.execute(text(query))
        activity_ids = [row[0] for row in result.fetchall()]
    return activity_ids


def update_streams_database(batch_size=50):
    """
    Met à jour la table streams pour les activités qui n'ont pas encore de streams

    Args:
        batch_size: Nombre d'activités à traiter par batch (défaut: 50)
    """
    activity_ids = get_activities_without_streams(limit=batch_size, recent_first=True)
    if not activity_ids:
        return "Toutes les activités ont déjà leurs streams"

    header = get_strava_header()
    streams_df = fetch_multiple_streams_df(activity_ids, header)

    if streams_df.empty:
        return f"Aucun stream récupéré pour {len(activity_ids)} activité(s) (probablement des workouts sans GPS)"

    store_df_streams_in_postgresql_optimized(
        streams_df,
        host=HOST,
        database=DATABASE,
        user=USER,
        password=PASSWORD,
        port=PORT
    )

    # Vérifier s'il reste des activités à traiter
    remaining = get_activities_without_streams(limit=1)
    status_msg = f"{len(streams_df)} stream(s) ajouté(s) pour {len(activity_ids)} activité(s)"
    if remaining:
        status_msg += f" - {len(remaining)} activité(s) restante(s) sans streams"
    else:
        status_msg += " - Toutes les activités ont maintenant leurs streams ✅"

    return status_msg


def update_all_streams_database():
    """
    Met à jour TOUS les streams manquants (utiliser avec précaution)
    """
    activity_ids = get_activities_without_streams()
    if not activity_ids:
        return "Toutes les activités ont déjà leurs streams"

    header = get_strava_header()
    streams_df = fetch_multiple_streams_df(activity_ids, header)

    if streams_df.empty:
        return f"Aucun stream récupéré pour {len(activity_ids)} activité(s)"

    store_df_streams_in_postgresql_optimized(
        streams_df,
        host=HOST,
        database=DATABASE,
        user=USER,
        password=PASSWORD,
        port=PORT
    )

    return f"{len(streams_df)} stream(s) ajouté(s) pour {len(activity_ids)} activité(s)"
