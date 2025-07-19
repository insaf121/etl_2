CREATE OR REPLACE PROCEDURE reload_account_balance_turnover()
LANGUAGE plpgsql
AS $$
DECLARE
    rows_updated INTEGER;
    rows_inserted INTEGER;
    start_time TIMESTAMP := clock_timestamp();
BEGIN
    -- Логирование начала процедуры
    RAISE NOTICE 'Начало процедуры перезагрузки витрины dm.account_balance_turnover: %', start_time;
    
    -- 1. Корректировка некорректных account_out_sum
    WITH corrected_data AS (
        SELECT 
            ab1.account_rk,
            ab1.effective_date,
            LEAD(ab2.account_in_sum) OVER (PARTITION BY ab1.account_rk ORDER BY ab1.effective_date) AS next_day_in_sum
        FROM 
            rd.account_balance ab1
        JOIN 
            rd.account_balance ab2 ON ab1.account_rk = ab2.account_rk 
                                  AND ab1.effective_date = ab2.effective_date - INTERVAL '1 day'
    )
    UPDATE rd.account_balance ab
    SET account_out_sum = cd.next_day_in_sum
    FROM corrected_data cd
    WHERE ab.account_rk = cd.account_rk
      AND ab.effective_date = cd.effective_date
      AND ab.account_out_sum IS DISTINCT FROM cd.next_day_in_sum;
    
    GET DIAGNOSTICS rows_updated = ROW_COUNT;
    RAISE NOTICE 'Обновлено % строк в rd.account_balance', rows_updated;
    
    -- 2. Очистка и перезагрузка витрины
    TRUNCATE TABLE dm.account_balance_turnover;
    
    INSERT INTO dm.account_balance_turnover
    SELECT 
        a.account_rk,
        COALESCE(dc.currency_name, '-1'::TEXT) AS currency_name,
        a.department_rk,
        ab.effective_date,
        ab.account_in_sum,
        ab.account_out_sum
    FROM 
        rd.account a
    LEFT JOIN 
        rd.account_balance ab ON a.account_rk = ab.account_rk
    LEFT JOIN 
        dm.dict_currency dc ON a.currency_cd = dc.currency_cd
    WHERE 
        ab.effective_date IS NOT NULL;
    
    GET DIAGNOSTICS rows_inserted = ROW_COUNT;
    
    -- Логирование завершения
    RAISE NOTICE 'Перезагрузка завершена. Вставлено % строк. Время выполнения: %', 
        rows_inserted, 
        clock_timestamp() - start_time;
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Ошибка при перезагрузке витрины: %', SQLERRM;
        -- Откат транзакции при ошибке
        ROLLBACK;
END;
$$;