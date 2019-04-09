SELECT *,
       CASE
           WHEN total_time > 60 THEN 'hard'
           WHEN total_time BETWEEN 31 AND 60 THEN 'medium'
           WHEN total_time <= 30 THEN 'easy'
           ELSE 'unknown'
       END AS difficulty,
       cast(CURRENT_DATE AS string)  AS date_of_execution
FROM
  (SELECT *,
          tominutes(preptime) + tominutes(cooktime) AS total_time
   FROM recipes) b
WHERE Lower(ingredients) LIKE '%beef%'