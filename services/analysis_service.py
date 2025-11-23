"""
Service pour les analyses avancées des activités
"""
import pandas as pd
import numpy as np
from services.activity_service import get_streams_for_activity


def calculate_rolling_hr_speed_correlation(activity_id: str, window_seconds: int = 180, min_periods_ratio: float = 0.5):
    """
    Calcule la corrélation glissante entre fréquence cardiaque et vitesse.

    Args:
        activity_id: ID de l'activité Strava
        window_seconds: Taille de la fenêtre glissante en secondes (défaut: 180s = 3min)
        min_periods_ratio: Ratio minimal de données non-nulles dans la fenêtre (défaut: 0.5)

    Returns:
        dict avec:
        - time: timestamps (secondes depuis début)
        - hr: fréquence cardiaque (bpm)
        - speed: vitesse (m/s)
        - hr_normalized: HR normalisée (z-score)
        - speed_normalized: vitesse normalisée (z-score)
        - correlation: corrélation glissante de Pearson
        - correlation_spearman: corrélation glissante de Spearman (robuste)
        - breakpoints: liste des instants de rupture (corr < -0.3)
    """
    # Récupérer les streams
    streams = get_streams_for_activity(activity_id)

    if not streams:
        return {"error": "Pas de données de streams disponibles"}

    # Convertir en DataFrame
    df = pd.DataFrame(streams)

    # Vérifier que nous avons les données nécessaires
    if 'heartrate' not in df.columns or 'velocity_smooth' not in df.columns:
        return {"error": "Données de fréquence cardiaque ou vitesse manquantes"}

    # Filtrer les lignes avec données valides
    df = df[['time_s', 'heartrate', 'velocity_smooth']].copy()
    df = df.dropna(subset=['time_s'])

    # Trier par temps
    df = df.sort_values('time_s').reset_index(drop=True)

    # Resample à 1 seconde avec interpolation
    # Créer une série temporelle complète de 0 à max(time_s)
    max_time = int(df['time_s'].max())
    time_range = pd.DataFrame({'time_s': range(0, max_time + 1)})

    # Merge et interpolation linéaire
    df = time_range.merge(df, on='time_s', how='left')
    df['heartrate'] = df['heartrate'].interpolate(method='linear', limit_direction='both')
    df['velocity_smooth'] = df['velocity_smooth'].interpolate(method='linear', limit_direction='both')

    # Filtrer les périodes d'arrêt (vitesse < 0.5 m/s = 1.8 km/h)
    df['is_moving'] = df['velocity_smooth'] > 0.5

    # Appliquer un filtre médian pour réduire le bruit GPS
    df['hr_filtered'] = df['heartrate'].rolling(window=5, center=True, min_periods=1).median()
    df['speed_filtered'] = df['velocity_smooth'].rolling(window=5, center=True, min_periods=1).median()

    # Normalisation z-score avec fenêtre longue (10 minutes)
    rolling_window_long = 600  # 10 minutes

    df['hr_mean_long'] = df['hr_filtered'].rolling(window=rolling_window_long, min_periods=60, center=True).mean()
    df['hr_std_long'] = df['hr_filtered'].rolling(window=rolling_window_long, min_periods=60, center=True).std()
    df['hr_normalized'] = (df['hr_filtered'] - df['hr_mean_long']) / (df['hr_std_long'] + 1e-6)

    df['speed_mean_long'] = df['speed_filtered'].rolling(window=rolling_window_long, min_periods=60, center=True).mean()
    df['speed_std_long'] = df['speed_filtered'].rolling(window=rolling_window_long, min_periods=60, center=True).std()
    df['speed_normalized'] = (df['speed_filtered'] - df['speed_mean_long']) / (df['speed_std_long'] + 1e-6)

    # Calculer la corrélation glissante de Pearson
    min_periods = int(window_seconds * min_periods_ratio)

    df['correlation_pearson'] = df['hr_normalized'].rolling(
        window=window_seconds,
        min_periods=min_periods
    ).corr(df['speed_normalized'])

    # Détecter les points de rupture
    # Critère: corrélation < -0.3 persistante sur au moins 2 fenêtres consécutives
    threshold = -0.3
    persistence_count = 2

    df['is_breakpoint'] = (df['correlation_pearson'] < threshold).rolling(
        window=persistence_count,
        min_periods=persistence_count
    ).sum() >= persistence_count

    # Extraire les instants de rupture
    breakpoints = df[df['is_breakpoint'] == True]['time_s'].tolist()

    # Préparer les données pour le frontend (sous-échantillonner si > 10000 points)
    if len(df) > 10000:
        # Garder 1 point tous les N points
        step = len(df) // 10000
        df_sampled = df.iloc[::step].copy()
    else:
        df_sampled = df.copy()

    # Convertir en format JSON-friendly avec conversion explicite en types Python natifs
    result = {
        "time": [float(x) if pd.notna(x) else None for x in df_sampled['time_s'].tolist()],
        "hr": [float(x) if pd.notna(x) else None for x in df_sampled['hr_filtered'].tolist()],
        "speed": [float(x) if pd.notna(x) else None for x in df_sampled['speed_filtered'].tolist()],
        "hr_normalized": [float(x) if pd.notna(x) else None for x in df_sampled['hr_normalized'].tolist()],
        "speed_normalized": [float(x) if pd.notna(x) else None for x in df_sampled['speed_normalized'].tolist()],
        "correlation_pearson": [float(x) if pd.notna(x) else None for x in df_sampled['correlation_pearson'].tolist()],
        "is_moving": [bool(x) if pd.notna(x) else False for x in df_sampled['is_moving'].tolist()],
        "breakpoints": [float(x) for x in breakpoints if x is not None],
        "window_seconds": int(window_seconds),
        "total_breakpoints": len([bp for bp in breakpoints if bp is not None])
    }

    return result
