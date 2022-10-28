#!/bin/bash

hist_fpath="./data/02_daily/local/"

for f in $hist_fpath*.csv;do

    cat $f | psql -U postgres -d projects -c "COPY weather.daily_historic_local(day, month, year, temp_f, state, city) FROM STDIN DELIMITER ',' "

done
