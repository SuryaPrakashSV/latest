WITH latest_tasks AS (
    SELECT
        "key" AS task_id,
        "title" AS task_title,
        "id" AS project_id,
        "status" AS general_status,
        "customStatusId" AS detailed_status,
        "updatedDate" AS updated_date,
        "completedDate" AS completed_date,
        "permalink" AS permalink
    FROM GBI_RETAIL_BAP_DB.ASO_OPS_WSA_SNDBX_BIZ_APP."Wrike_STG"
    WHERE NULLIF(TRIM("key"), '') IS NOT NULL
      AND UPPER(TRIM("key")) <> 'PLACEHOLDER'
      AND "key" IN (
          "parent_task_id",
          "child_task_id",
          "grandchild_task_id",
          "baby_task_id",
          "grandbaby_task_id",
          "great_grandbaby_task_id"
      )
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY "key"
        ORDER BY
            TRY_TO_TIMESTAMP_TZ("updatedDate"::VARCHAR)
                DESC NULLS LAST,
            "id"
    ) = 1
)
SELECT *
FROM latest_tasks
WHERE LOWER(TRIM(general_status)) = 'active'
  AND LOWER(TRIM(detailed_status)) IN (
      'new',
      'not started',
      'pending',
      'in progress'
  )
ORDER BY
    CASE LOWER(TRIM(detailed_status))
        WHEN 'new' THEN 1
        WHEN 'not started' THEN 2
        WHEN 'pending' THEN 3
        WHEN 'in progress' THEN 4
    END,
    TRY_TO_TIMESTAMP_TZ(updated_date::VARCHAR) DESC NULLS LAST,
    task_id
LIMIT 10;
