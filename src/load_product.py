import csv
from database import get_db_connection
from get_csv_path import get_csv_path



def load_product():
    csv_path = get_csv_path('product_info.csv')
    
    conn = None
    try:
        
        conn = get_db_connection()
        cur = conn.cursor()
        # 1. Создаем временную таблицу без ограничений
        cur.execute("""
            CREATE TEMP TABLE tmp_info (
                LIKE rd.product INCLUDING DEFAULTS
            )
        """)

        # 2. Загружаем данные из CSV
        with open(csv_path, 'r', encoding='windows-1251') as f:
            reader = csv.DictReader(f, delimiter=',')
            
            for row in reader:
                # Преобразуем пустые строки в NULL
                cleaned_row = {
                    k: (None if v == '' else v) 
                    for k, v in row.items()
                }
                
                # Вставка данных во временную таблицу
                cur.execute("""
                    INSERT INTO tmp_info VALUES (
                        %(product_rk)s,
                        %(product_name)s,
                        %(effective_from_date)s,
                        %(effective_to_date)s
                    )
                """, cleaned_row)

        # 3. Определяем количество новых записей
        cur.execute("""
            SELECT COUNT(DISTINCT t.*)
            FROM tmp_info t
            WHERE NOT EXISTS (
                SELECT 1 FROM rd.product d
                WHERE t.product_rk = d.product_rk
                AND t.effective_from_date = d.effective_from_date
                AND t.effective_to_date = d.effective_to_date
                AND COALESCE(t.product_name, '') = COALESCE(d.product_name, '')
            )
        """)
        new_records = cur.fetchone()[0]
        print(f"Найдено {new_records} новых записей")


        # 4. Добавляем только уникальные новые записи
        if new_records > 0:
            cur.execute("""
                INSERT INTO rd.product
                SELECT DISTINCT t.*
                FROM tmp_info t
                WHERE NOT EXISTS (
                    SELECT 1 FROM rd.product d
                    WHERE t.product_rk = d.product_rk
                    AND t.effective_from_date = d.effective_from_date
                    AND t.effective_to_date = d.effective_to_date
                    AND COALESCE(t.product_name, '') = COALESCE(d.product_name, '')
                )
                LIMIT 2        
            """)
            print(f"Добавлено {cur.rowcount} новых уникальных записей")
        else:
            print("Нет новых записей для добавления")

        conn.commit()
        print("Операция завершена успешно!")

    except Exception as e:
        print(f"Ошибка: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    load_product()