CREATE MATERIALIZED VIEW IF NOT EXISTS weather.mv_temp AS (
WITH weather_tbl AS (
	SELECT fd.datetime AS Date,
			fd.latitude AS Latitude,
			fd.longitude AS Longitude,
			fd.tempmax AS Max_Temperature,
			fd.tempmin AS Min_Temperature,
			ROUND(CAST((fd.tempmin + fd.tempmax)/2 AS numeric), 1) AS Avg_Temperature
	FROM weather.f_daily fd
	UNION
	SELECT fm.datetime AS Date,
			fm.latitude AS Latitude,
			fm.longitude AS Longitude,
			fm.tmax AS Max_Temperature,
			fm.tmin AS Min_Temperature,
			fm.tavg AS Avg_Temperature
	FROM weather.f_hist fm
)

SELECT ROW_NUMBER() OVER () AS ID, 
		w.*,
		resolvedaddress as City
FROM weather_tbl w
JOIN weather.dim_location dl 
ON CAST(dl.latitude AS DOUBLE PRECISION) = w.latitude AND CAST(dl.longitude AS DOUBLE PRECISION) = w.longitude
);


