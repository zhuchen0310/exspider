## 设计目标：

- 开箱即用、灵活部署（可以快速部署到任何一台 Linux 服务器）
- 抓取数据连续、完整、不丢数据
- 抓取运行状态清晰、可控
- 常见问题自动修复
- 实时数据抓取 Trade(Websocket+RESTful)，历史数据通过 RESTful 抓取不同 TimeFrame OHLCV
- OHLCV TimeFrame 覆盖最小 1min： 1min 3min 5min 10min 15min 30min 1h 2h 4h 6h 12h 1d 3d 1week 1month
- 按照交易/币种热门程度，执行不同的更新频率，有用户订阅的交易对更新最实时


## 抓取原则：

- 尽量抓取最小粒度的 TimeFrame，其他粒度通过最小粒度计算
- 不重复抓取，冗余的抓取只用作数据校验和回补


## 受限因素：

- 交易所可供查询历史数据有限
- AICoin 1min 覆盖度有限（只有最近1周）
- 交易所 API 访问频率有限制


## 主要模块：

- 爬虫（spider）
    - 连接 dispatch 注册
    - 接收 dispatcher 的指令
    - 通过 Websocket 实时抓取 Trade 数据
    - 通过 RESTful 抓取近期历史数据
    - 数据先存储在本地：本地文件 or leveldb 
    - 定期将本地数据上传至 storekeeper
    - 实时上报运行状态至 dispatcher：IP、Exchange、Pair List
    - 以 Docker 方式运行
- 中央控制器（dispatcher）
    - 统计 spider 运行状态
    - 将交易所抓取请求均摊到各个 spider
    - spider 运行出错时及时切换任务
    - 协调 IP / 服务器资源
    - Admin Dashboard：运行时间，spider 数量，IP 数量，交易所数量，交易对数量，K线条数，Trade条数
- 存储服务器（storekeeper）
    - 接收 spider 上传的数据，持久化存储
    - 根据数据热度存储到不同数据库（Redis、PostgreSQL、OSS）

