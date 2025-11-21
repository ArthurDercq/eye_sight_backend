"""
Script to backfill new stream data for existing activities.

This script will:
1. Get all activity IDs from the database
2. Fetch the new stream data (heartrate, cadence, velocity_smooth, temp, power, grade_smooth)
3. Update existing stream records with new data using PostgreSQL UPDATE queries

Run this after the migration to add new stream columns.
"""

import sys
import os
from psycopg2 import connect
from psycopg2.extras import execute_values
import pandas as pd
import time

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from strava.fetch_strava import get_strava_header, fetch_stream, get_all_activity_ids_from_db
from strava.params import HOST, DATABASE, USER, PASSWORD, PORT, TABLE_NAME


def get_activities_in_streams(conn):
    """Get unique activity IDs that already have streams in the database."""
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT activity_id FROM streams ORDER BY activity_id")
        return [row[0] for row in cur.fetchall()]


def update_streams_with_new_data(conn, df_stream, activity_id):
    """
    Update existing stream records with new data fields.
    Uses UPDATE queries to add the new stream data to existing rows.
    """
    if df_stream.empty:
        print(f"  ⚠️  Pas de données stream pour l'activité {activity_id}")
        return 0

    with conn.cursor() as cur:
        # Prepare update data
        updates = []
        for _, row in df_stream.iterrows():
            updates.append((
                row.get('heartrate'),
                row.get('cadence'),
                row.get('velocity_smooth'),
                row.get('temp'),
                row.get('power'),
                row.get('grade_smooth'),
                str(activity_id),
                float(row['time_s'])
            ))

        # Batch update with explicit type casting
        update_query = """
            UPDATE streams
            SET heartrate = data.heartrate::INTEGER,
                cadence = data.cadence::INTEGER,
                velocity_smooth = data.velocity_smooth::DOUBLE PRECISION,
                temp = data.temp::INTEGER,
                power = data.power::INTEGER,
                grade_smooth = data.grade_smooth::DOUBLE PRECISION
            FROM (VALUES %s) AS data(heartrate, cadence, velocity_smooth, temp, power, grade_smooth, activity_id, time_s)
            WHERE streams.activity_id = data.activity_id
            AND streams.time_s = data.time_s::DOUBLE PRECISION
        """

        execute_values(cur, update_query, updates)
        updated_count = cur.rowcount

    return updated_count


def backfill_streams(start_from=None, max_activities=None, max_per_15min=590):
    """
    Backfill new stream data for all existing activities.

    Args:
        start_from: Activity ID to start from (useful for resuming)
        max_activities: Maximum number of activities to process (None = all)
        max_per_15min: Max API calls per 15 minutes to respect Strava rate limits
    """
    print("🚀 Démarrage du backfill des streams...\n")

    # Connect to database
    conn = connect(
        host=HOST,
        database=DATABASE,
        user=USER,
        password=PASSWORD,
        port=PORT
    )
    conn.autocommit = False

    try:
        # Get Strava API header
        print("🔑 Authentification Strava...")
        header = get_strava_header()

        # Get all activities that have streams
        print("📋 Récupération des activités ayant des streams...")
        activity_ids = get_activities_in_streams(conn)
        print(f"   ✅ {len(activity_ids)} activités trouvées\n")

        # Filter by start_from if specified
        if start_from:
            activity_ids = [aid for aid in activity_ids if str(aid) >= str(start_from)]
            print(f"   ⏩ Démarrage depuis l'activité {start_from}")
            print(f"   📊 {len(activity_ids)} activités à traiter\n")

        # Limit if specified
        if max_activities:
            activity_ids = activity_ids[:max_activities]
            print(f"   🎯 Limitation à {max_activities} activités\n")

        # Process activities
        total_updated = 0
        api_calls = 0
        start_time = time.time()

        for i, activity_id in enumerate(activity_ids, 1):
            # Rate limiting
            if api_calls >= max_per_15min:
                elapsed = time.time() - start_time
                wait_time = 15 * 60 - elapsed

                if wait_time > 0:
                    print(f"\n⏸  Limite Strava atteinte ({max_per_15min} appels)")
                    print(f"   ⏳ Pause de {int(wait_time/60)} minutes...")
                    time.sleep(wait_time)

                # Reset counters
                api_calls = 0
                start_time = time.time()

            try:
                print(f"[{i}/{len(activity_ids)}] Activité {activity_id}...")

                # Fetch stream data
                df_stream = fetch_stream(activity_id, header)
                api_calls += 1

                # Update database
                updated = update_streams_with_new_data(conn, df_stream, activity_id)
                conn.commit()

                total_updated += updated
                print(f"  ✅ {updated} lignes mises à jour")

            except Exception as e:
                print(f"  ❌ Erreur: {e}")
                conn.rollback()
                continue

        print(f"\n✅ Backfill terminé!")
        print(f"   📊 Total: {total_updated} lignes mises à jour")
        print(f"   🔄 {len(activity_ids)} activités traitées")
        print(f"   📞 {api_calls} appels API effectués")

    except Exception as e:
        print(f"\n❌ Erreur fatale: {e}")
        import traceback
        traceback.print_exc()

    finally:
        conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Backfill new stream data for existing activities")
    parser.add_argument("--start-from", type=str, help="Activity ID to start from (resume)")
    parser.add_argument("--max", type=int, help="Maximum number of activities to process")
    parser.add_argument("--rate-limit", type=int, default=590, help="Max API calls per 15 minutes (default: 590)")

    args = parser.parse_args()

    backfill_streams(
        start_from=args.start_from,
        max_activities=args.max,
        max_per_15min=args.rate_limit
    )
