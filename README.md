## manager.py:

- NatsManager 负责触发事件驱动，推送ratio
- ExchangeManager 负责对接api
- OrderManager 负责下单，组合ExchangeManager

## utils.py

- EventSystem 负责订阅各种事件，包括ratio,order update,position update等事件
- OrderResponse 封装的order返回
- MarketDataStore 负责计算ratio

## bot.py

- TradingBot: 实现交易逻辑

## main.py
- main 主函数