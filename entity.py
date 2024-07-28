import sys
import pickle
import asyncio


from loguru import logger
from pathlib import Path
from collections import defaultdict, deque
from dataclasses import dataclass, fields, field
from typing import Dict, List, Callable, Any, Literal


@dataclass
class OrderResponse:
    __slots__ = ['id', 'symbol', 'status', 'side', 'amount', 'filled', 'last_filled', 'remaining', 'client_order_id', 'average', 'price']
    
    id: str
    symbol: str
    status: str
    side: Literal['buy', 'sell']
    amount: float
    filled: float
    last_filled: float 
    remaining: float
    client_order_id: str
    average: float
    price: float
    
    def __getitem__(self, key):
        if hasattr(self, key):
            return getattr(self, key)
        raise KeyError(key)

    def __setitem__(self, key, value):
        if hasattr(self, key):
            setattr(self, key, value)
        else:
            raise KeyError(key)

    def keys(self):
        return [f.name for f in fields(self)]

    def __iter__(self):
        return iter(self.keys())

    def __len__(self):
        return len(fields(self))
    
    def get(self, key: str, default: Any = None) -> Any:
        if hasattr(self, key):
            return getattr(self, key)
        return default
    
class Quote:
    __slots__ = ['ask', 'bid']

    def __init__(self, ask: float = 0, bid: float = 0):
        self.ask = ask
        self.bid = bid

    def __getitem__(self, key):
        if key == 'ask':
            return self.ask
        elif key == 'bid':
            return self.bid
        else:
            raise KeyError(f"Invalid key: {key}")

    def __setitem__(self, key, value):
        if key == 'ask':
            self.ask = value
        elif key == 'bid':
            self.bid = value
        else:
            raise KeyError(f"Invalid key: {key}")

    def __repr__(self):
        return f"Quote(ask={self.ask}, bid={self.bid})"   
    

class EventSystem:
    _listeners: Dict[str, List[Callable]] = {}

    @classmethod
    def on(cls, event: str, callback: Callable):
        if event not in cls._listeners:
            cls._listeners[event] = []
        cls._listeners[event].append(callback)

    @classmethod
    async def emit(cls, event: str, *args: Any, **kwargs: Any):
        if event in cls._listeners:
            for callback in cls._listeners[event]:
                if asyncio.iscoroutinefunction(callback):
                    await callback(*args, **kwargs)
                else:
                    callback(*args, **kwargs)

@dataclass
class Account:
    USDT: float = 0
    BNB: float = 0
    FDUSD: float = 0
    BTC: float = 0
    ETH: float = 0
    USDC: float = 0

    def __init__(self, account_type: str):
        self.filepath = Path('.context') / f'{account_type}.pkl'
        self.load_account()

    def __post_init__(self):
        # dataclass自动调用此方法，此处用于跳过dataclass自动初始化
        pass

    def __setattr__(self, key, value):
        object.__setattr__(self, key, value)
        if key in self.keys():
            self.save_account()

    def __getitem__(self, key):
        if key in self.keys():
            return getattr(self, key)
        else:
            raise KeyError(f"{key} is not a valid account field.")

    def __setitem__(self, key, value):
        if key in self.keys():
            setattr(self, key, value)
        else:
            raise KeyError(f"{key} is not a valid account field.")

    def keys(self):
        return [f.name for f in fields(self)]

    def save_account(self):
        """Save account data to a pickle file."""
        filepath = self.filepath
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with filepath.open('wb') as file:
            pickle.dump({field: getattr(self, field) for field in self.keys()}, file)

    def load_account(self):
        """Load account data from a pickle file, if it exists."""
        filepath = self.filepath
        if filepath.exists():
            with filepath.open('rb') as file:
                data = pickle.load(file)
                for key, value in data.items():
                    setattr(self, key, value)
        else:
            # 如果文件不存在，则初始化所有货币为0
            for field in self.keys():
                setattr(self, field, 0)

@dataclass
class Position:
    symbol: str = None
    amount: float = 0.0
    last_price: float = 0.0
    avg_price: float = 0.0
    total_cost: float = 0.0

    def update(self, order_amount, order_price):
        self.total_cost += order_amount * order_price
        self.amount += order_amount
        self.avg_price = self.total_cost / self.amount if self.amount != 0 else 0
        self.last_price = order_price

class PositionDict(Dict[str, Position]):
    def __init__(self):
        super().__init__()
        self.file_path = Path(".context/positions.pkl")
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.load_positions()

    def __setitem__(self, key: str, value: Position):
        super().__setitem__(key, value)
        self.save_positions()

    def update(self, symbol: str, order_amount: float, order_price: float):
        if symbol not in self:
            self[symbol] = Position(symbol=symbol)
        self[symbol].update(order_amount, order_price)
        
        if abs(self[symbol].amount) <= 1e-8:
            del self[symbol]
            
        self.save_positions()

    def load_positions(self):
        if self.file_path.exists():
            with self.file_path.open('rb') as f:
                positions = pickle.load(f)
                for key, value in positions.items():
                    self[key] = value

    def save_positions(self):
        with self.file_path.open('wb') as f:
            pickle.dump(dict(self), f)
        

class Context:
    def __init__(self):
        self._data = {}
        self.spot_account = Account('spot_account')
        self.futures_account = Account('futures_account')
        self.position = PositionDict()
        self._load_data()

    def __repr__(self) -> str:
        attributes = [f"{k}: {v}" for k, v in self._data.items()]
        base_repr = f"Spot Account: {self.spot_account}\nFutures Account: {self.futures_account}\nPositions: {self.position}"
        return base_repr + "\n" + "\n".join(attributes)

    def __setattr__(self, name, value):
        if name in ['spot_account', 'futures_account', 'position', '_data']:
            super().__setattr__(name, value)
        else:
            self._data[name] = value
            self._save_data()

    def __getattr__(self, name):
        if name in self._data:
            return self._data[name]
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    def _get_data_path(self):
        return Path('.context') / 'data.pkl'

    def _save_data(self):
        path = self._get_data_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('wb') as f:
            pickle.dump(self._data, f)

    def _load_data(self):
        path = self._get_data_path()
        if path.exists():
            with path.open('rb') as f:
                self._data = pickle.load(f)


class RollingMedian:
    def __init__(self, n = 50):
        self.n = n
        self.data = set()
        self.queue = []

    def input(self, value):
        if len(self.queue) == self.n:
            old_value = self.queue.pop(0)
            if old_value not in self.queue:
                self.data.remove(old_value)
        
        self.queue.append(value)
        self.data.add(value)
        
        return self.get_median()

    def get_median(self):
        sorted_data = sorted(self.data)
        length = len(sorted_data)
        if length % 2 == 0:
            return (sorted_data[length//2 - 1] + sorted_data[length//2]) / 2
        else:
            return sorted_data[length//2]
        
        
class MarketDataStore:
    quote:Dict[str, Quote] = defaultdict(Quote)
    open_ratio = {}
    close_ratio = {}
    open_rolling_median = defaultdict(RollingMedian)
    close_rolling_median = defaultdict(RollingMedian)
    
    @classmethod
    async def update(cls, data: Dict):
        symbol = data['s']
        cls.quote[symbol] = Quote(
            ask=float(data['a']),
            bid=float(data['b'])
        )
        
        spot_symbol = symbol.replace(':USDT', '') if ':' in symbol else symbol
        await cls.calculate_ratio(spot_symbol)
            
    
    @classmethod
    async def calculate_ratio(cls, spot_symbol: str):
        linear_symbol = f'{spot_symbol}:USDT'
        if spot_symbol in cls.quote and linear_symbol in cls.quote:
            spot_bid = cls.quote[spot_symbol].bid
            spot_ask = cls.quote[spot_symbol].ask
            linear_bid = cls.quote[linear_symbol].bid
            linear_ask = cls.quote[linear_symbol].ask
            
            # cls.close_ratio[spot_symbol] = linear_bid / spot_bid - 1
            # cls.open_ratio[spot_symbol] = linear_ask / spot_ask - 1
            
            # cls.open_ratio[spot_symbol] = linear_bid / spot_ask - 1
            # cls.close_ratio[spot_symbol] = linear_ask / spot_bid - 1
            
            cls.open_ratio[spot_symbol] = cls.open_rolling_median[spot_symbol].input(linear_ask / spot_ask - 1)
            cls.close_ratio[spot_symbol] = cls.close_rolling_median[spot_symbol].input(linear_bid / spot_bid - 1)
            
            await EventSystem.emit('ratio_changed', spot_symbol, cls.open_ratio[spot_symbol], cls.close_ratio[spot_symbol])


# class LogRegister:
#     def __init__(self, log_dir=".logs"):
#         self.log_dir = Path(log_dir)
#         self.log_dir.mkdir(parents=True, exist_ok=True)
#         self.loggers = {}

#     def get_logger(self, class_name, level=Literal['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']):
#         if class_name not in self.loggers:
#             log_file = self.log_dir / f"{class_name}.log"
#             logger_instance = logger.bind(class_name=class_name)
#             logger.add(str(log_file), 
#                        filter=lambda record: record["extra"].get("class_name") == class_name,
#                        rotation="1 day",
#                        level=level)
#             self.loggers[class_name] = logger_instance
#         return self.loggers[class_name]

class LogRegister:
    def __init__(self, log_dir=".logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.loggers = {}
        
        # 移除默认的处理器
        logger.remove()
        
        # 设置全局日志
        self.setup_global_logging()
        
        # 设置全局异常处理
        self.setup_exception_handling()

    def setup_global_logging(self, level='INFO'):
        # 添加控制台处理器
        logger.add(sys.stderr, level=level)
        
        # 添加全局日志文件处理器
        logger.add(str(self.log_dir / "log.log"), level=level, rotation="1 day")
        
        # 添加错误日志文件处理器
        logger.add(str(self.log_dir / "error.log"), level="ERROR", rotation="1 day")

    def setup_exception_handling(self):
        def handle_exception(exc_type, exc_value, exc_traceback):
            # 忽略 KeyboardInterrupt 异常
            if issubclass(exc_type, KeyboardInterrupt):
                sys.__excepthook__(exc_type, exc_value, exc_traceback)
                return
            
            # 记录未捕获的异常
            logger.opt(exception=(exc_type, exc_value, exc_traceback)).error("Uncaught exception:")

        # 设置全局异常处理器
        sys.excepthook = handle_exception

        # 确保异步代码中的异常也被捕获
        logger.add(lambda _: None, level="ERROR", backtrace=True, diagnose=True)

    def get_logger(self, class_name, level=Literal['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']):
        if class_name not in self.loggers:
            log_file = self.log_dir / f"{class_name}.log"
            logger_instance = logger.bind(class_name=class_name)
            logger.add(str(log_file), 
                       filter=lambda record: record["extra"].get("class_name") == class_name,
                       rotation="1 day",
                       level=level)
            self.loggers[class_name] = logger_instance
        return self.loggers[class_name]



context = Context()
log_register = LogRegister()
