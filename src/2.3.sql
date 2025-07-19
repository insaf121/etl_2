WITH corrected_in_sum AS (
    SELECT 
        ab1.account_rk,
        ab1.effective_date,
        CASE 
            WHEN ab1.account_in_sum != LAG(ab2.account_out_sum) OVER (PARTITION BY ab1.account_rk ORDER BY ab1.effective_date) 
            THEN LAG(ab2.account_out_sum) OVER (PARTITION BY ab1.account_rk ORDER BY ab1.effective_date)
            ELSE ab1.account_in_sum
        END AS corrected_account_in_sum
    FROM 
        rd.account_balance ab1
    JOIN 
        rd.account_balance ab2 ON ab1.account_rk = ab2.account_rk 
                              AND ab1.effective_date = ab2.effective_date + INTERVAL '1 day'
)
SELECT * FROM corrected_in_sum;


WITH corrected_out_sum AS (
    SELECT 
        ab1.account_rk,
        ab1.effective_date,
        CASE 
            WHEN ab1.account_out_sum != LEAD(ab2.account_in_sum) OVER (PARTITION BY ab1.account_rk ORDER BY ab1.effective_date) 
            THEN LEAD(ab2.account_in_sum) OVER (PARTITION BY ab1.account_rk ORDER BY ab1.effective_date)
            ELSE ab1.account_out_sum
        END AS corrected_account_out_sum
    FROM 
        rd.account_balance ab1
    JOIN 
        rd.account_balance ab2 ON ab1.account_rk = ab2.account_rk 
                              AND ab1.effective_date = ab2.effective_date - INTERVAL '1 day'
)
SELECT * FROM corrected_out_sum;


UPDATE rd.account_balance ab
SET account_in_sum = corrected.corrected_account_in_sum
FROM (
    SELECT 
        ab1.account_rk,
        ab1.effective_date,
        CASE 
            WHEN ab1.account_in_sum != LAG(ab2.account_out_sum) OVER (PARTITION BY ab1.account_rk ORDER BY ab1.effective_date) 
            THEN LAG(ab2.account_out_sum) OVER (PARTITION BY ab1.account_rk ORDER BY ab1.effective_date)
            ELSE ab1.account_in_sum
        END AS corrected_account_in_sum
    FROM 
        rd.account_balance ab1
    JOIN 
        rd.account_balance ab2 ON ab1.account_rk = ab2.account_rk 
                              AND ab1.effective_date = ab2.effective_date + INTERVAL '1 day'
) corrected
WHERE ab.account_rk = corrected.account_rk 
  AND ab.effective_date = corrected.effective_date 
  AND ab.account_in_sum != corrected.corrected_account_in_sum;



CREATE OR REPLACE PROCEDURE reload_account_balance_turnover()
LANGUAGE SQL
AS $$
    -- Обновляем некорректные account_out_sum (по правилу из п.2)
    UPDATE rd.account_balance ab
    SET account_out_sum = corrected.corrected_account_out_sum
    FROM (
        SELECT 
            ab1.account_rk,
            ab1.effective_date,
            CASE 
                WHEN ab1.account_out_sum != LEAD(ab2.account_in_sum) OVER (PARTITION BY ab1.account_rk ORDER BY ab1.effective_date) 
                THEN LEAD(ab2.account_in_sum) OVER (PARTITION BY ab1.account_rk ORDER BY ab1.effective_date)
                ELSE ab1.account_out_sum
            END AS corrected_account_out_sum
        FROM 
            rd.account_balance ab1
        JOIN 
            rd.account_balance ab2 ON ab1.account_rk = ab2.account_rk 
                                    AND ab1.effective_date = ab2.effective_date - INTERVAL '1 day'
    ) corrected
    WHERE ab.account_rk = corrected.account_rk 
      AND ab.effective_date = corrected.effective_date 
      AND ab.account_out_sum != corrected.corrected_account_out_sum;

    -- Перезагружаем витрину (согласно прототипу)
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
        dm.dict_currency dc ON a.currency_cd = dc.currency_cd;
$$;