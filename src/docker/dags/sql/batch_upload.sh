#!/bin/bash

hist_fpath="./data/01_historic/local/data_load/universityofdayton/"

for f in $hist_fpath*.csv;do
    cat $f | psql -U postgres -d projects -c "COPY weather.daily_historic_local_ud(month, day, year, temp_f, state, city) FROM STDIN DELIMITER ',' "
done

hist_fpath="./data/01_historic/local/data_load/meteostat/"

for f in $hist_fpath*.csv;do
    cat $f | psql -U postgres -d projects -c "COPY weather.daily_historic_local_m(time, tavg, tmin, tmax, prcp, snow, wdir, wspd, wpgt, pres, tsun, lat, lon, add) FROM STDIN DELIMITER ',' CSV HEADER;"
done