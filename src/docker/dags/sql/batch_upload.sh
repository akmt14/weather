#!/bin/bash

hist_fpath="./data/01_historic/local/"

for f in $hist_fpath*.csv;do
    cat $f | psql -U postgres -d projects -c "COPY weather.raw_daily_historic_local(time, tavg, tmin, tmax, prcp, snow, wdir, wspd, wpgt, pres, tsun, lat, lon, add) FROM STDIN DELIMITER ',' CSV HEADER;"
done