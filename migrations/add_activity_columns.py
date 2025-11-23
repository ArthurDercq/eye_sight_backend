"""
Migration script to add new activity columns to the activites table.

Adds the following columns:
- kilojoules (DOUBLE PRECISION): Total energy output in kilojoules
- average_watts (DOUBLE PRECISION): Average power output in watts
- device_watts (BOOLEAN): Whether the power data is from a device
- max_watts (INTEGER): Maximum power output in watts
- weighted_average_watts (INTEGER): Weighted average power (for cycling)
- average_heartrate (DOUBLE PRECISION): Average heart rate in bpm
- max_heartrate (INTEGER): Maximum heart rate in bpm
- average_cadence (DOUBLE PRECISION): Average cadence
- average_temp (INTEGER): Average temperature in Celsius
- has_heartrate (BOOLEAN): Whether activity has heart rate data
- elev_high (DOUBLE PRECISION): Highest elevation point in meters
- elev_low (DOUBLE PRECISION): Lowest elevation point in meters
- pr_count (INTEGER): Number of personal records achieved
- total_photo_count (INTEGER): Number of photos
- suffer_score (INTEGER): Strava suffer score

Run this script to update your database schema before fetching new activity data.
"""

import sys
import os
from psycopg2 import connect, sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Add parent directory to path to import params
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from strava.params import HOST, DATABASE, USER, PASSWORD, PORT


def run_migration():
    """Add new columns to the activites table."""

    print("🔄 Starting migration: Adding new activity columns...")

    # Connect to database
    conn = connect(
        host=HOST,
        database=DATABASE,
        user=USER,
        password=PASSWORD,
        port=PORT
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    try:
        with conn.cursor() as cur:
            # Check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = 'activites'
                );
            """)

            table_exists = cur.fetchone()[0]

            if not table_exists:
                print("❌ La table 'activites' n'existe pas encore.")
                print("   Créez d'abord des activités avec l'ancien système.")
                return False

            # Define new columns to add
            new_columns = [
                ("kilojoules", "DOUBLE PRECISION", "Total energy output in kilojoules"),
                ("average_watts", "DOUBLE PRECISION", "Average power output in watts"),
                ("device_watts", "BOOLEAN", "Whether power data is from a device"),
                ("max_watts", "INTEGER", "Maximum power output in watts"),
                ("weighted_average_watts", "INTEGER", "Weighted average power"),
                ("average_heartrate", "DOUBLE PRECISION", "Average heart rate in bpm"),
                ("max_heartrate", "INTEGER", "Maximum heart rate in bpm"),
                ("average_cadence", "DOUBLE PRECISION", "Average cadence"),
                ("average_temp", "INTEGER", "Average temperature in Celsius"),
                ("has_heartrate", "BOOLEAN", "Whether activity has heart rate data"),
                ("elev_high", "DOUBLE PRECISION", "Highest elevation point in meters"),
                ("elev_low", "DOUBLE PRECISION", "Lowest elevation point in meters"),
                ("pr_count", "INTEGER", "Number of personal records achieved"),
                ("total_photo_count", "INTEGER", "Number of photos"),
                ("suffer_score", "INTEGER", "Strava suffer score")
            ]

            # Check which columns already exist
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = 'activites';
            """)

            existing_columns = {row[0] for row in cur.fetchall()}
            print(f"📋 Colonnes existantes: {sorted(existing_columns)}")

            # Add missing columns
            columns_added = 0
            for col_name, col_type, description in new_columns:
                if col_name not in existing_columns:
                    print(f"  ➕ Ajout de la colonne '{col_name}' ({col_type})...")

                    alter_query = sql.SQL("""
                        ALTER TABLE activites
                        ADD COLUMN {} {}
                    """).format(
                        sql.Identifier(col_name),
                        sql.SQL(col_type)
                    )

                    cur.execute(alter_query)

                    # Add comment on column
                    comment_query = sql.SQL("""
                        COMMENT ON COLUMN activites.{} IS %s
                    """).format(sql.Identifier(col_name))

                    cur.execute(comment_query, (description,))

                    columns_added += 1
                    print(f"  ✅ Colonne '{col_name}' ajoutée")
                else:
                    print(f"  ⏭️  Colonne '{col_name}' existe déjà")

            # Display final schema
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = 'activites'
                ORDER BY ordinal_position;
            """)

            final_columns = cur.fetchall()

            print(f"\n✅ Migration terminée!")
            print(f"   {columns_added} nouvelle(s) colonne(s) ajoutée(s)")
            print(f"\n📊 Schéma final de la table 'activites':")
            for col_name, col_type in final_columns:
                print(f"   - {col_name}: {col_type}")

            return True

    except Exception as e:
        print(f"❌ Erreur lors de la migration: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        conn.close()


if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
