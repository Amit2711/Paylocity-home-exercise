{{ config(materialized='view') }}

-- 1. Show Alice Johnson's full salary history with change tracking.
SELECT
    EMPLOYEE_ID,
    FULL_NAME,
    DEPARTMENT,
    SALARY,
    VALID_FROM,
    VALID_TO,
    LAG(SALARY) OVER (PARTITION BY EMPLOYEE_ID ORDER BY VALID_FROM) AS PREVIOUS_SALARY
FROM {{ ref('employee_history') }}
WHERE EMPLOYEE_ID IN (
    SELECT EMPLOYEE_ID
    FROM {{ ref('employee_current_state') }}
    WHERE FULL_NAME = 'Alice Johnson'
)
ORDER BY VALID_FROM

/*
Results:
EMPLOYEE_ID  FULL_NAME      DEPARTMENT   SALARY  VALID_FROM           VALID_TO             PREVIOUS_SALARY
101          Alice Johnson  Engineering  75000   2024-01-10 09:00:00  2024-02-15 14:00:00  NULL
101          Alice Johnson  Engineering  85000   2024-02-15 14:00:00  2024-03-01 08:00:00  75000
101          Alice Johnson  Engineering  85000   2024-03-01 08:00:00  2024-03-20 11:00:00  85000
101          Alice Johnson  Platform     95000   2024-03-20 11:00:00  NULL                 85000
*/
