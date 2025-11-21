"""
Migration script to add new stream columns to the streams table.

Adds the following columns:
- heartrate (INTEGER): Heart rate in bpm
- cadence (INTEGER): Running cadence or cycling cadence
- velocity_smooth (DOUBLE PRECISION): Smoothed velocity in m/s
- temp (INTEGER): Temperature in Celsius
- power (INTEGER): Power in watts
- grade_smooth (DOUBLE PRECISION): Smoothed grade percentage

Run this script to update your database schema before fetching new stream data.
"""

import sys
import os
from psycopg2 import connect, sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Add parent directory to path to import params
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from strava.params import HOST, DATABASE, USER, PASSWORD, PORT


def run_migration():
    """Add new columns to the streams table."""

    print("🔄 Starting migration: Adding new stream columns...")

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
                    AND table_name = 'streams'
                );
            """)

            table_exists = cur.fetchone()[0]

            if not table_exists:
                print("❌ La table 'streams' n'existe pas encore.")
                print("   Créez d'abord des streams avec l'ancien système.")
                return False

            # Define new columns to add
            new_columns = [
                ("heartrate", "INTEGER", "Heart rate in bpm"),
                ("cadence", "INTEGER", "Running/cycling cadence"),
                ("velocity_smooth", "DOUBLE PRECISION", "Smoothed velocity in m/s"),
                ("temp", "INTEGER", "Temperature in Celsius"),
                ("power", "INTEGER", "Power in watts"),
                ("grade_smooth", "DOUBLE PRECISION", "Smoothed grade percentage")
            ]

            # Check which columns already exist
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = 'streams';
            """)

            existing_columns = {row[0] for row in cur.fetchall()}
            print(f"📋 Colonnes existantes: {existing_columns}")

            # Add missing columns
            columns_added = 0
            for col_name, col_type, description in new_columns:
                if col_name not in existing_columns:
                    print(f"  ➕ Ajout de la colonne '{col_name}' ({col_type})...")

                    alter_query = sql.SQL("""
                        ALTER TABLE streams
                        ADD COLUMN {} {}
                    """).format(
                        sql.Identifier(col_name),
                        sql.SQL(col_type)
                    )

                    cur.execute(alter_query)

                    # Add comment on column
                    comment_query = sql.SQL("""
                        COMMENT ON COLUMN streams.{} IS %s
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
                AND table_name = 'streams'
                ORDER BY ordinal_position;
            """)

            final_columns = cur.fetchall()

            print(f"\n✅ Migration terminée!")
            print(f"   {columns_added} nouvelle(s) colonne(s) ajoutée(s)")
            print(f"\n📊 Schéma final de la table 'streams':")
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
