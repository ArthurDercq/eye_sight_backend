from db.connection import get_conn
import pandas as pd
from services.activity_service import get_all_activities

def test_get_all_activities():
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM activites ORDER BY start_date DESC;")
                rows = cur.fetchall()
                print(f"{len(rows)} activités récupérées")

                # Affiche les 5 premières lignes pour vérifier
                #for row in rows[:5]:
                #    print(row)

                # Convertir en DataFrame pour voir la structure
                df = pd.DataFrame(rows)
                print(df.head())

    except Exception as e:
        print("Erreur lors de la récupération des activités :", e)

if __name__ == "__main__":
    #test_get_all_activities()

    df = get_all_activities()
    print(df.head())
    print(df.isna().sum())
    print(df.dtypes)
