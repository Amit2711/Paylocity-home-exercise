{% snapshot employee_snapshot %}

{{
    config(
        target_schema           = target.schema,
        unique_key              = 'employee_id',
        strategy                = 'timestamp',
        updated_at              = 'effective_as_of',
        invalidate_hard_deletes = True
    )
}}

-- alternative way to track history using dbt's built-in snapshot
-- dbt adds dbt_valid_from / dbt_valid_to automatically
-- run with: dbt snapshot
--
-- i kept this as an option but employee_history is the main one i use
-- snapshots only capture state at run time so you lose what happened between runs
-- employee_history rebuilds from raw events so it handles late data better

SELECT *
FROM {{ ref('employee_current_state') }}

{% endsnapshot %}
