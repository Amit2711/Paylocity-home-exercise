{{ config(materialized='view') }}

-- 1. What is the salary of employee 102?
SELECT EMPLOYEE_ID, FULL_NAME, DEPARTMENT, SALARY
FROM {{ ref('employee_current_state') }}
WHERE EMPLOYEE_ID = 102
  AND IS_ACTIVE = TRUE

/*
Results:
EMPLOYEE_ID  FULL_NAME   DEPARTMENT  SALARY
102          Bob Smith   Marketing   72000
*/
