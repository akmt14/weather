-- * DAILY TABLE MANIPULATION

-- fixing address
UPDATE weather.raw_daily_api_local SET address = 'Birmingham' WHERE address ILIKE 'Birmingham Al';
UPDATE weather.raw_daily_api_local SET address = 'Burlington' WHERE address ILIKE 'Burlington Vt';
UPDATE weather.raw_daily_api_local SET address = 'Columbia' WHERE address ILIKE 'Columbia Sc';
UPDATE weather.raw_daily_api_local SET address = 'Daytona' WHERE address ILIKE 'Daytona Beach';
UPDATE weather.raw_daily_api_local SET address = 'Erie' WHERE address ILIKE 'Erie Pennsylvania';
UPDATE weather.raw_daily_api_local SET address = 'Fort Worth' WHERE address ILIKE 'Ft Worth';
UPDATE weather.raw_daily_api_local SET address = 'Miami' WHERE address ILIKE 'Miami Beach';
UPDATE weather.raw_daily_api_local SET address = 'Midland Odessa' WHERE address ILIKE '%Midland%';
UPDATE weather.raw_daily_api_local SET address = 'Minneapolis' WHERE address ILIKE 'Minneapolis St Paul';
UPDATE weather.raw_daily_api_local SET address = 'Providence' WHERE address ILIKE 'Providence Ri';
UPDATE weather.raw_daily_api_local SET address = 'Providence' WHERE address ILIKE 'Rhode Island';
UPDATE weather.raw_daily_api_local SET address = 'Salem' WHERE address ILIKE 'Salem Ma';
UPDATE weather.raw_daily_api_local SET address = 'Waco' WHERE address ILIKE 'Waco Texas';
UPDATE weather.raw_daily_api_local SET address = 'Washington' WHERE address ILIKE 'Washington DC';
UPDATE weather.raw_daily_api_local SET address = 'Wilkes Barre' WHERE address ILIKE 'Wilkes';
UPDATE weather.raw_daily_api_local SET address = 'Yuma' WHERE address ILIKE 'Yuma Az';

-- discarding non US values
DELETE FROM weather.raw_daily_api_local WHERE resolvedaddress NOT ILIKE '%United States';


-- * HISTORIC TABLE MANIPULATION

-- discarding records with missing temperatues
DELETE FROM weather.raw_daily_historic_local_ud WHERE temp_f = -99.0;

-- fixing address
UPDATE weather.raw_daily_historic_local_ud SET city = 'Washington' WHERE city ILIKE 'Washington DC';
UPDATE weather.raw_daily_historic_local_ud SET city = 'Tampa St Petersburg' WHERE city ILIKE 'Tampa%';