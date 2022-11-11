INSERT INTO weather.f_hist_ud SELECT u.id, 
                                    MAKE_DATE(CAST(u.year AS INT),
                                    CAST(u.month AS INT),
                                    CAST(u.day AS INT)) as datetime,
                                    u.temp_f,
                                    CAST(l.latitude AS DOUBLE PRECISION) AS lat,
                                    CAST(l.longitude  AS DOUBLE PRECISION)  AS lon
                                FROM weather.raw_daily_historic_local_ud u
                                LEFT JOIN weather.dim_location l ON TRIM(UPPER(u.city)) = TRIM(UPPER(l.address))
                                WHERE NOT EXISTS (SELECT * FROM weather.f_hist_ud);

INSERT INTO weather.f_hist_m SELECT m.id, 
                                    CAST(latitude AS DOUBLE PRECISION),
                                    CAST(longitude AS DOUBLE PRECISION),
                                    time,
                                    tavg,
                                    tmin,
                                    tmax,
                                    prcp,
                                    snow,
                                    wdir,
                                    wspd,
                                    wpgt,
                                    pres,
                                    tsun
                            FROM weather.raw_daily_historic_local_m m
                            LEFT JOIN weather.dim_location l ON TRIM(UPPER(split_part(m.add, ',', 1))) = TRIM(UPPER(l.address))
                            WHERE NOT EXISTS (SELECT * FROM weather.f_hist_m);

INSERT INTO weather.f_daily SELECT id,
                                CAST(latitude AS DOUBLE PRECISION),
                                CAST(longitude AS DOUBLE PRECISION),
                                datetime, 
                                tempmax, 
                                tempmin, 
                                feelslikemax, 
                                feelslikemin, 
                                feelslike,
                                dew,
                                humidity,
                                precip,
                                precipprob,
                                precipcover,
                                preciptype,
                                snow,
                                snowdepth,
                                windgust,
                                windspeed,
                                winddir,
                                pressure,
                                cloudcover,
                                visibility,
                                solarradiation,
                                solarenergy,
                                uvindex,
                                severerisk,
                                sunrise,
                                sunset,
                                moonphase,
                                conditions,
                                description 
                            FROM weather.raw_daily_api_local rdal 
                            WHERE NOT EXISTS ( SELECT d_id FROM weather.f_daily WHERE d_id = rdal.id);