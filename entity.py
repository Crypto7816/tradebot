import logging
import pickle
import asyncio


from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass, fields, field
from typing import Dict, List, Callable, Any, Literal


@dataclass
class OrderResponse:
    __slots__ = ['id', 'symbol', 'status', 'side', 'amount', 'filled', 'last_filled', 'client_order_id', 'average', 'price']
    
    id: str
    symbol: str
    status: str
    side: Literal['buy', 'sell']
    amount: float
    filled: float
    last_filled: float 
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
    
    
class MarketDataStore:
    quote:Dict[str, Quote] = defaultdict(Quote)
    open_ratio = {}
    close_ratio = {}
    
    @classmethod
    async def update(cls, data: Dict):
        symbol = data['s']
        cls.quote[symbol] = Quote(
            ask=float(data['a']),
            bid=float(data['b'])
        )
        
        logging.debug(f"Updated {symbol} bid: {data['b']} ask: {data['a']}")
        
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
            
            cls.open_ratio[spot_symbol] = linear_bid / spot_ask - 1
            cls.close_ratio[spot_symbol] = linear_ask / spot_bid - 1
            
            await EventSystem.emit('ratio_changed', spot_symbol, cls.open_ratio[spot_symbol], cls.close_ratio[spot_symbol])


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

class PositionDict(Dict):
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
        self.spot_account = Account('spot_account')
        self.futures_account = Account('futures_account')
        self.position = PositionDict()
    
    def __repr__(self) -> str:
        return f"Spot Account: {self.spot_account}\nFutures Account: {self.futures_account}\nPositions: {self.position}"


context = Context()









    

    
    
    

    
