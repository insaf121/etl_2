import psycopg2
from dotenv import load_dotenv
import os

load_dotenv(".env")

try:
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        port=os.getenv('DB_PORT')
    )
    print(" Подключение успешно!")
    cur = conn.cursor()
    cur.execute("SELECT nspname FROM pg_catalog.pg_namespace;")
    print(cur.fetchall())  # Список всех схем
    cur = conn.cursor()
    cur.execute("SELECT current_database();")
    print("Текущая БД:", cur.fetchone()[0])
    print("DB_NAME из .env:", os.getenv('DB_NAME'))  # Должно быть 'dwh'
    print("Текущая рабочая папка:", os.getcwd())  

    conn.close()
except Exception as e:
    print(f" Ошибка подключения: {e}")