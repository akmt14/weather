#!/bin/bash

hist_fpath="./data/01_historic/local/data_load/"

for f in $hist_fpath*.csv;do
    cat $f | psql -U postgres -d projects -c "COPY weather.daily_historic_local(month, day, year, temp_f, state, city) FROM STDIN DELIMITER ',' "
done