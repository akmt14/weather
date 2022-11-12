-- RAW TABLES

CREATE TABLE IF NOT EXISTS weather.raw_daily_api_local
(
    id SERIAL PRIMARY KEY NOT NULL,
	queryCost INT,
	latitude TEXT,
	longitude TEXT,
	resolvedAddress TEXT,
	address TEXT,
	timezone TEXT,
	tzoffset DOUBLE PRECISION,
	datetime DATE,
	datetimeEpoch NUMERIC,
	tempmax DOUBLE PRECISION,
	tempmin DOUBLE PRECISION,
	feelslikemax DOUBLE PRECISION,
	feelslikemin DOUBLE PRECISION,
	feelslike DOUBLE PRECISION,
	dew DOUBLE PRECISION,
	humidity DOUBLE PRECISION,
	precip DOUBLE PRECISION,
    precipprob DOUBLE PRECISION,
    precipcover DOUBLE PRECISION,
    preciptype TEXT,
    snow DOUBLE PRECISION,
    snowdepth DOUBLE PRECISION,
    windgust DOUBLE PRECISION,
    windspeed DOUBLE PRECISION,
    winddir DOUBLE PRECISION,
    pressure DOUBLE PRECISION,
    cloudcover DOUBLE PRECISION,
    visibility DOUBLE PRECISION,
    solarradiation DOUBLE PRECISION,
    solarenergy DOUBLE PRECISION,
    uvindex DOUBLE PRECISION,
    severerisk DOUBLE PRECISION,
    sunrise TEXT,
    sunriseEpoch NUMERIC,
    sunset TEXT,
    sunsetEpoch NUMERIC,
    moonphase DOUBLE PRECISION,
    conditions TEXT,
    description TEXT,
    icon TEXT,
	station JSON
    );

    
CREATE TABLE IF NOT EXISTS weather.raw_daily_historic_local
(
    id SERIAL PRIMARY KEY NOT NULL,
    "time" DATE,
    tavg DOUBLE PRECISION,
    tmin DOUBLE PRECISION,
    tmax DOUBLE PRECISION,
    prcp DOUBLE PRECISION,
    snow DOUBLE PRECISION,
    wdir DOUBLE PRECISION,
    wspd DOUBLE PRECISION,
    wpgt DOUBLE PRECISION,
    pres DOUBLE PRECISION,
    tsun DOUBLE PRECISION,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    "add" TEXT
);


-- DIM TABLES

CREATE TABLE IF NOT EXISTS weather.d_us_states
(
    id SERIAL PRIMARY KEY NOT NULL,
    abb  TEXT,
    "state" TEXT,
    region TEXT
    );

CREATE TABLE IF NOT EXISTS weather.d_us_cities(
    citykey SERIAL PRIMARY KEY NOT NULL,
    cityname TEXT UNIQUE,
    citylat TEXT,
    citylon TEXT
);

-- FACT TABLES

CREATE TABLE IF NOT EXISTS weather.f_daily(
    d_id INT PRIMARY KEY NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    datetime DATE,
    tempmax DOUBLE PRECISION,
	tempmin DOUBLE PRECISION,
	feelslikemax DOUBLE PRECISION,
	feelslikemin DOUBLE PRECISION,
	feelslike DOUBLE PRECISION,
	dew DOUBLE PRECISION,
	humidity DOUBLE PRECISION,
	precip DOUBLE PRECISION,
    precipprob DOUBLE PRECISION,
    precipcover DOUBLE PRECISION,
    preciptype TEXT,
    snow DOUBLE PRECISION,
    snowdepth DOUBLE PRECISION,
    windgust DOUBLE PRECISION,
    windspeed DOUBLE PRECISION,
    winddir DOUBLE PRECISION,
    pressure DOUBLE PRECISION,
    cloudcover DOUBLE PRECISION,
    visibility DOUBLE PRECISION,
    solarradiation DOUBLE PRECISION,
    solarenergy DOUBLE PRECISION,
    uvindex DOUBLE PRECISION,
    severerisk DOUBLE PRECISION,
    sunrise TEXT,
    sunset TEXT,
    moonphase DOUBLE PRECISION,
    conditions TEXT,
    description TEXT
    );

CREATE TABLE IF NOT EXISTS weather.f_hist(
    m_id INT PRIMARY KEY NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    datetime DATE,
    tavg DOUBLE PRECISION,
    tmin DOUBLE PRECISION,
    tmax DOUBLE PRECISION,
    prcp DOUBLE PRECISION,
    snow DOUBLE PRECISION,
    wdir DOUBLE PRECISION,
    wspd DOUBLE PRECISION,
    wpgt DOUBLE PRECISION,
    pres DOUBLE PRECISION,
    tsun DOUBLE PRECISION
    );
