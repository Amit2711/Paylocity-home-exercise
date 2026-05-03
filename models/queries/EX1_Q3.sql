{{ config(materialized='view') }}

-- 3. Who has the highest salary?
SELECT EMPLOYEE_ID, FULL_NAME, DEPARTMENT, SALARY
FROM {{ ref('employee_current_state') }}
WHERE IS_ACTIVE = TRUE
ORDER BY SALARY DESC
LIMIT 1

/*
Results:
EMPLOYEE_ID  FULL_NAME      DEPARTMENT  SALARY
101          Alice Johnson  Platform    95000
*/
