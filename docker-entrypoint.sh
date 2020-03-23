#!/bin/bash

# set -eo pipefail
# shopt -s nullglob

pip install -r requirements.txt

for var in $*
do
    # echo "$var"
    supervisor_conf_file="exspider/supervisor_conf/"
    if [[ $var == *"price_alarm"* ]]
    then
        # echo "incoude price_alarm"
        supervisor_conf_file=${supervisor_conf_file}"price_alarm/"
    elif [[ $var == *"spider"* ]]
    then
        # echo "incoude spider"
        supervisor_conf_file=${supervisor_conf_file}"spiders/"
    elif [[ $var == *"storekeeper"* ]]
    then
        # echo "incoude storekeeper"
        supervisor_conf_file=${supervisor_conf_file}"storekeeper/"
    # else
        # echo "not incoude"
    fi
    supervisor_conf_file=${supervisor_conf_file}$var".conf"
    echo "supervisord -c "${supervisor_conf_file}
    # supervisord -c exspider/supervisor_conf/storekeeper_test.conf
    # supervisord -c exspider/supervisor_conf/price_alarm/pair_price_alarm_test.conf
    supervisord -c ${supervisor_conf_file}
done

python manage.py start_prometheus_server

# while true
# do
#     sleep 1
# done

# exec "$@"
