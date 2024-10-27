CREATE OR REPLACE MACRO dedupe_func(
      unique_cols,
      order_by_col
  ) AS TABLE (
    WITH ranked_tbl AS (
      SELECT 
        *
        , row_number() OVER (
          PARTITION BY COLUMNS( c -> (list_contains(unique_cols, c)) )
          ORDER BY order_by_col DESC
          -- ORDER BY COLUMNS (order_by_col) fails with Parser Error
          -- "Cannot ORDER BY ALL in a window expression"
          ) AS row_rank
      FROM __input_cte
    )
    SELECT
      * EXCLUDE (row_rank)
    FROM ranked_tbl
    WHERE row_rank = 1
  )
;
