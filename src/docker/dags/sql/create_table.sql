CREATE TABLE IF NOT EXISTS weather.daily_api_local
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

CREATE TABLE IF NOT EXISTS weather.daily_historic_local
(
    id SERIAL PRIMARY KEY NOT NULL,
	"month" TEXT,
	"day" TEXT,
	"year" TEXT,
	temp_f DOUBLE PRECISION,
    "state" TEXT,
    city TEXT
    );

CREATE TABLE IF NOT EXISTS weather.dim_us_states
(
    id SERIAL PRIMARY KEY NOT NULL,
    abb  TEXT,
    "state" TEXT,
    region TEXT
    );
    