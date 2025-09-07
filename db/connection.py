# db/connection.py
import os
from contextlib import contextmanager
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine

# Charger les variables d'environnement depuis .env
load_dotenv()

DATABASE = os.getenv("DATABASE")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST", "localhost")
PORT = int(os.getenv("PORT", 5432))


@contextmanager
def get_conn():
    """
    Context manager pour une connexion psycopg2.
    Utilise RealDictCursor pour retourner des résultats sous forme de dict.
    """
    conn = psycopg2.connect(
        dbname=DATABASE,
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        cursor_factory=RealDictCursor
    )
    try:
        yield conn
    finally:
        conn.close()
