
### 爬虫运行命令

- ##### 环境待选参数
```python

ENV = [dev, storekeeper, spider_kline]
```

- ##### 设置待启动交易所 / 添加交易对

```python

# 不支持同时设置多个交易对, 多个交易对.
# 多个交易所 交易对 用, 隔开

# 状态 start 会将新交易所对添加 并设置为待启动状态
# 状态 stop 会将指定交易对 设置为人工停止

# 添加交易所所有交易对
    python manage.py init_exchange_symbols okex

# 停止所有交易对
    python manage.py set_spider huobipro '' kline stop

# 设置一个新的交易对
    python manage.py set_spider huobipro btcusdt kline start
    
# 设置多个交易所
    python manage.py set_spider huobipro,binance '' kline start

# 设置多个交易对
    python manage.py set_spider huobipro btcusdt,ethusdt kline start

# 启动 抓取:
    python manage.py start_exspider huobipro kline


# 统一管理的shell 命令:
 ~    ./start_exspider.sh local_dev|dev bibox btcusdt|多个用,隔开 trade|kline 
```

- ##### 启动AICOIN 监听
```python

python manage.py care_aicoin_spider kline
```

- ##### 启动自动监控server
```python
# 8001
python manage.py start_prometheus_server
```

- ##### 启动生成csv任务
```python
python manage.py save_all_csv '' '' kline 1

```

- ##### 将csv 导入postgres
```python
| python manage.py storekeeper import_trade huobipro btcusdt
| python manage.py storekeeper import_ohlcv huobipro btcusdt

# python6 导入命令
~ docker exec -it exspider python manage.py stdin_data /data1/trades/hk1/ okex btcusdt trade storekeeper

```

- ##### 使用docker 构建
```python
# 启动指定 一个 docker-comprose 
1、 ./docker.sh run docker-comprose.yml     #创建一个镜像并启动容器，如果镜像已经创建过了会直接启动容器。
2、 ./docker.sh restart docker-comprose.yml #修改配置文件后，通过此名命令可重新加载容器运行
3、 ./docker.sh rm docker-comprose.yml    #删除容器
4、 ./docker.sh drun docker-comprose.yml    #后台运行容器
5、 ./docker.sh logs docker-comprose.yml   #在后台运行时，通过此命令查看运行的内容

进入容器:

~ docker exec -it exspider /bin/bash

执行命令:

~ docker exec -it exspider python manage.py runserver

启动supervisor:
~ docker exec -it exspider supervisord -c exspider/supervisor_conf/spiders/spider1.conf

```

- ##### 导入aicoin 所有交易对key, 用户请求kline, 和关系映射
```python
python manage.py init_aicoin_pair
```

- ##### celery 启动
```python

celery -B -A storekeeper worker -l info [ENV]
```

- ##### 测试 mq 消费者
```python
python manage.py test_consumer [ENV]
```

- ##### 价格异动的启动:
~ supervisord -c exspider/supervisor_conf/price_alarm/pair_price_alarm_prod.conf

- ##### 回测价格异动:
~ python manage.py test_price_update binance btc_usdt 550 local_dev # 回测binance 历史500条数据
