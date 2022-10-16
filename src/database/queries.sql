-- CREATE unlogged TABLE IF NOT EXISTS weather_import (doc JSON)

-- COPY weather_import FROM '/Users/akshaykamath/Documents/Project/weather/data/02_daily/local/2022-10-15/LOUISVILLE.json'

DROP TABLE IF EXISTS weather_daily_local;

CREATE TABLE IF NOT EXISTS weather_daily_local
    (id SERIAL PRIMARY KEY NOT NULL,
    queryCost int NOT NULL,
	 address text NOT NULL,
	 "datetime" date NOT NULL,
	 datetimeEpoch INT NOT NULL,
	 "temp" FLOAT NOT NULL
    );


WITH weather_daily_local (doc) AS (VALUES(
'
[{
"queryCost": 1,
"address": "St Louis",
"days": [
	{
		"datetime": "2022-10-15",
		"datetimeEpoch": 1665810000,
		"temp": 65.9
	}
	]
	}
]
	'::json)
), expand AS (
  SELECT json_array_elements(doc) AS doc
   FROM weather_daily_local
)
INSERT INTO weather_daily_local (queryCost, address, "datetime", datetimeEpoch, "temp")
SELECT (doc->>'queryCost')::INT as queryCost,
       doc->>'address' as address,
	   (doc->'days'->0->>'datetime')::DATE AS "datetime",
	   (doc->'days'->0->>'datetimeEpoch')::INT AS datetimeEpoch,
       (doc->'days'->0->>'temp')::FLOAT AS "temp"
FROM expand;
  
SELECT * FROM weather_daily_local