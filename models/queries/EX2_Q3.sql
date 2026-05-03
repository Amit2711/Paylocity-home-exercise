{{ config(materialized='view') }}

-- 3. What department was Alice Johnson in when she first started?
-- resolving name -> employee_id via current_state first to avoid a full history scan
-- using IN instead of = just in case there are two people with the same name
SELECT H.EMPLOYEE_ID, H.FULL_NAME, H.DEPARTMENT, H.SALARY
FROM {{ ref('employee_history') }} H
WHERE H.EMPLOYEE_ID IN (
    SELECT EMPLOYEE_ID
    FROM {{ ref('employee_current_state') }}
    WHERE FULL_NAME = 'Alice Johnson'
)
QUALIFY ROW_NUMBER() OVER (PARTITION BY H.EMPLOYEE_ID ORDER BY H.VALID_FROM ASC) = 1

/*
Results:
EMPLOYEE_ID  FULL_NAME      DEPARTMENT   SALARY
101          Alice Johnson  Engineering  75000
*/
