#!/usr/bin/env bash
BASEPATH=$(cd "$(dirname "$0")"; pwd)
file_dir=$(pwd)
file=`basename $file_dir`

case $1 in
    local_dev)
        echo $1 $2 $3 $4
        #  local_dev binance btcusdt kline
        python $BASEPATH/manage.py set_spider $1 $2 all $4 stop
        python $BASEPATH/manage.py set_spider $1 $2 $3 $4 start
        python $BASEPATH/manage.py start_exspider $1 $2 $4
        ;;
    '')
        python $BASEPATH/manage.py set_spider $2 all $4 stop
        python $BASEPATH/manage.py set_spider $2 $3 $4 start
        python $BASEPATH/manage.py start_exspider $2 $4
        ;;
    *)
      echo "Usage: $0 [local_dev|dev|''] binance [btcusdt|all] kline"
      exit 1
      ;;
esac
exit 0

