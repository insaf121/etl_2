import csv
from database import get_db_connection
from get_csv_path import get_csv_path



def load_deal_info():
    csv_path = get_csv_path('deal_info.csv')
    
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TEMP TABLE tmp_deal_info (
                LIKE rd.deal_info INCLUDING DEFAULTS
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
                    INSERT INTO tmp_deal_info VALUES (
                        %(deal_rk)s,
                        %(deal_num)s,
                        %(deal_name)s,
                        %(deal_sum)s,
                        %(client_rk)s,
                        %(account_rk)s,
                        %(agreement_rk)s,
                        %(deal_start_date)s,
                        %(department_rk)s,
                        %(product_rk)s,
                        %(deal_type_cd)s,
                        %(effective_from_date)s,
                        %(effective_to_date)s
                    )
                """, cleaned_row)

        # 3. Определяем количество новых записей
        cur.execute("""
            SELECT COUNT(DISTINCT t.*)
            FROM tmp_deal_info t
            WHERE NOT EXISTS (
                SELECT 1 FROM rd.deal_info d
                WHERE t.deal_rk = d.deal_rk
                AND t.effective_from_date = d.effective_from_date
                AND t.effective_to_date = d.effective_to_date
                AND COALESCE(t.deal_num, '') = COALESCE(d.deal_num, '')
                AND COALESCE(t.deal_name, '') = COALESCE(d.deal_name, '')
                AND COALESCE(t.deal_sum, 0) = COALESCE(d.deal_sum, 0)
            )
        """)
        new_records = cur.fetchone()[0]
        print(f"Найдено {new_records} новых записей")


        # 4. Добавляем только уникальные новые записи
        if new_records > 0:
            cur.execute("""
                INSERT INTO rd.deal_info
                SELECT DISTINCT t.*
                FROM tmp_deal_info t
                WHERE NOT EXISTS (
                    SELECT 1 FROM rd.deal_info d
                    WHERE t.deal_rk = d.deal_rk
                    AND t.effective_from_date = d.effective_from_date
                    AND t.effective_to_date = d.effective_to_date
                    AND COALESCE(t.deal_num, '') = COALESCE(d.deal_num, '')
                    AND COALESCE(t.deal_name, '') = COALESCE(d.deal_name, '')
                    AND COALESCE(t.deal_sum, 0) = COALESCE(d.deal_sum, 0)
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
    load_deal_info()