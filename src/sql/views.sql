
CREATE MATERIALIZED VIEW weather.mv_daily_avg_temp AS (
WITH address AS (
	SELECT DISTINCT resolvedaddress,
		TRIM(CASE  
			WHEN split_part(resolvedaddress, ',', 2) ILIKE '%Minneapolis%' THEN 'MN'
			WHEN split_part(resolvedaddress, ',', 2) ILIKE '%Petersburg%' THEN 'FL'
			WHEN split_part(resolvedaddress, ',', 2) ILIKE '%Iola%' THEN 'TX'
			ELSE split_part(resolvedaddress, ',', 2)
		END) AS add_state_abb, 
		Trim(initcap(
            CASE 
                WHEN address ILIKE 'Birmingham Al' THEN 'Birmingham'
                WHEN address ILIKE 'Burlington Vt' THEN 'Burlington'
                WHEN address ILIKE 'Columbia Sc' THEN 'Columbia'
                WHEN address ILIKE 'Daytona Beach' THEN 'Daytona'
                WHEN address ILIKE 'Erie Pennsylvania' THEN 'Erie'
                WHEN address ILIKE 'Ft Worth' THEN 'Fort Worth'
                WHEN address ILIKE 'Miami Beach' THEN 'Miami'
                WHEN address ILIKE 'Midland Odessa' THEN 'Midland'
                WHEN address ILIKE 'Minneapolis St Paul' THEN 'Minneapolis'
                WHEN address ILIKE 'Providence Ri' THEN 'Providence'
                WHEN address ILIKE 'Rhode Island' THEN 'Providence'
                WHEN address ILIKE 'Salem Ma' THEN 'Salem'
                WHEN address ILIKE 'Waco Texas' THEN 'Waco'
                WHEN address ILIKE 'Washington DC' THEN 'Washington'
                WHEN address ILIKE 'Yuma Az' THEN 'Yuma'
                ELSE address
            END
        )) AS add_city
        
		FROM weather.daily_api_local
	),
	
hist AS (	
SELECT MAKE_DATE(CAST(year AS INT),
		CAST(month AS INT),
		CAST(day AS INT)) as datetime,
		temp_f AS avg_temp, 
		city,
		regexp_replace(dal.state, '([a-z])([A-Z])', '\1 \2','g') AS state,
		CASE 
			WHEN regexp_replace(dal.state, '([a-z])([A-Z])', '\1 \2','g') IN ('Pennsylvania','Maryland') THEN 'Middle Atlantic'
			WHEN regexp_replace(dal.state, '([a-z])([A-Z])', '\1 \2','g') = 'Michigan' THEN 'Midwest'
			WHEN regexp_replace(dal.state, '([a-z])([A-Z])', '\1 \2','g') = 'Florida' THEN 'South'
			WHEN regexp_replace(dal.state, '([a-z])([A-Z])', '\1 \2','g') = 'Texas' THEN 'Southwest'
			ELSE region
		END AS region
	FROM weather.daily_historic_local dal
	LEFT JOIN address a ON a.add_city = dal.city
	LEFT JOIN weather.dim_us_states dus ON dus.abb = a.add_state_abb
),
daily AS (

	SELECT datetime,
		round(cast((tempmin + tempmax)/2 AS numeric), 1) AS avg_temp,
		a.add_city AS city,
		dus.state,
		dus.region
	FROM weather.daily_api_local dal
	LEFT JOIN address a ON a.resolvedaddress = dal.resolvedaddress
	LEFT JOIN weather.dim_us_states dus ON dus.abb = a.add_state_abb)

SELECT * FROM (SELECT * FROM daily
UNION
SELECT * FROM hist) AS temp
)