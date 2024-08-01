import time
import asyncio
from typing import List, Dict


from collections import defaultdict


from utils import spot_2_linear, linear_2_spot, is_linear, generate_client_order_id
from entity import context, log_register
from entity import EventSystem, MarketDataStore, OrderResponse
from manager import NatsManager, OrderManager, ExchangeManager, AccountManager

class TradingBot:
    logger = log_register.get_logger('bot', level='INFO', flush=True)
    
    def __init__(self, config):
        self._config = config
        self._exchange = ExchangeManager(config)
        self._order = OrderManager(self._exchange)
        self._account = AccountManager()
        self._nats = NatsManager()
        
        
        EventSystem.on('new_order', self._on_new_order)
        EventSystem.on('filled_order', self._on_filled_order)
        EventSystem.on('partially_filled_order', self._on_partially_filled_order)
        EventSystem.on('canceled_order', self._on_canceled_order)
        
    async def run(self):
        await self._exchange.load_markets()
        asyncio.create_task(self._nats.subscribe())
        asyncio.create_task(self._exchange.watch_user_data_stream())
        await self._wait()
    
    async def _wait(self):
        await asyncio.Event().wait()
        
    async def _on_new_order(self, order):
        if hasattr(self, 'on_new_order'):
            await self.on_new_order(order)
    
    async def _on_filled_order(self, order):
        if hasattr(self, 'on_filled_order'):
            await self.on_filled_order(order)
    
    async def _on_partially_filled_order(self, order):
        if hasattr(self, 'on_partially_filled_order'):
            await self.on_partially_filled_order(order)
    
    async def _on_canceled_order(self, order):
        if hasattr(self, 'on_canceled_order'):
            await self.on_canceled_order(order)

class Bot(TradingBot):
    def __init__(self, config):
        super().__init__(config)
        self.client_id = generate_client_order_id()
        self.order_ids = {}
        self.trade_log = log_register.get_logger('trade', level='INFO')
        context.openpx = defaultdict(float)
        context.level_time = defaultdict(int)
        self.pending_tasks: Dict[str, asyncio.Task] = {}
        EventSystem.on('ratio_changed', self.on_ratio_changed)

    async def on_new_order(self, order: OrderResponse):
        if order.client_order_id == self.client_id and is_linear(order.symbol):
        # if is_linear(order.symbol):
            id = order.id
            self.order_ids[id] = order.filled
            self._order.logger.info(f'[NEW ORDER] id: {id} Symbol: {order.symbol} Amount: {order.amount} Side: {order.side}')

    async def on_partially_filled_order(self, order: OrderResponse):
        if order.client_order_id == self.client_id and is_linear(order.symbol):
        # if is_linear(order.symbol):
            id = order.id
            filled = order.filled
            symbol = linear_2_spot(order.symbol)
            linear_average = order.average
            amount = filled - self.order_ids[id]
            res = await self.order_spot(order, symbol, amount)
            spot_average = res['average']
            context.openpx[symbol] = linear_average/spot_average - 1
            self.order_ids[id] = filled - amount + res['filled']
            self.logger.info(f'[PARTIALLY FILLED ORDER] id: {id} Symbol: {symbol} Amount: {res["filled"]} Basis: {context.openpx[symbol]} Already Filled: {self.order_ids[id]}')
            
        
    async def on_filled_order(self, order: OrderResponse):
        if order.client_order_id == self.client_id and is_linear(order.symbol):
        # if is_linear(order.symbol):
            id = order.id
            symbol = linear_2_spot(order.symbol)
            filled = order.filled
            linear_average = order.average
            if id in self.order_ids:
                amount = filled - self.order_ids[id]
                res = await self.order_spot(order, symbol, amount)
                spot_average = res['average']
                context.openpx[symbol] =  linear_average/spot_average - 1
                self.logger.info(f'[FILLED ORDER] id: {id} Symbol: {symbol} Amount: {amount} Filled: {res["filled"]} Basis: {context.openpx[symbol]}')
                self.order_ids.pop(id, None)
            else:
                self.logger.info(f'[SOCKET DELAY] Symbol: {symbol} already filled')
                
    async def on_canceled_order(self, order: OrderResponse):
        if order.client_order_id == self.client_id and is_linear(order.symbol):
        # if is_linear(order.symbol):
            id = order.id 
            self.order_ids.pop(id, None)
            self._order.logger.info(f'[CANCELED ORDER] id: {id} Symbol: {order.symbol} Amount: {order.amount} Side: {order.side}')
        
    
    async def on_ratio_changed(self, symbol: str, open_ratio: float, close_ratio: float):
        spread_ratio = 0.00065
        time_ratio = 2
        
        mask_open = open_ratio > spread_ratio and symbol not in context.position
        mask_diverge = close_ratio < context.openpx[symbol] - spread_ratio * time_ratio ** context.level_time[symbol] and symbol in context.position
        
        if symbol not in self.pending_tasks:
            task = asyncio.create_task(self._process_symbol(symbol, mask_diverge, mask_open, open_ratio, close_ratio), name=symbol)
            self.pending_tasks[symbol] = task

    async def _process_symbol(self, symbol: str, mask_diverge: bool, mask_open: bool, open_ratio: float, close_ratio: float):
        try:
            if mask_diverge:
                self.logger.info(f"Closing position for {symbol} at {close_ratio}")
                await self.order_linear(
                    symbol=symbol,
                    amount=context.position[symbol].amount,
                    close_position=True,
                    open_ratio=close_ratio,
                )
            elif mask_open:
                self.logger.info(f"Opening position for {symbol} at {open_ratio}")
                await self.order_linear(
                    symbol=symbol,
                    notional=20,
                    open_ratio=open_ratio,
                )
            self.pending_tasks.pop(symbol, None)
        except Exception as e:
            self.logger.error(f"Error processing {symbol}: {e}")
    
    
    async def order_linear_test(
        self,
        symbol: str, 
        amount: float = None,
        notional: float = None,
        close_position: bool = False,
        open_ratio: float = None,
        wait: int = 120,
    ):
        await asyncio.sleep(wait)
        linear_symbol = spot_2_linear(symbol)
        if close_position:
            spot_bid = MarketDataStore.quote[symbol].bid
            linear_bid = MarketDataStore.quote[linear_symbol].bid            
            context.position.update(symbol, -amount, spot_bid)
            context.position.update(linear_bid ,amount, linear_bid)
        else:
            spot_ask = MarketDataStore.quote[symbol].ask
            linear_ask = MarketDataStore.quote[linear_symbol].ask
            amount = notional / linear_ask
            
            context.position.update(symbol, amount, spot_ask)
            context.position.update(linear_symbol, -amount, linear_ask)
            
        context.openpx[symbol] = open_ratio
        self.logger.info(f'[FILLED ORDER]: {symbol} ratio: {open_ratio}')
            
    
    
    async def order_linear(
        self,
        symbol: str, # spot
        amount: float = None,
        notional: float = None,
        time_interval: int = 0.05,
        close_position: bool = False,
        open_ratio: float = None,
        wait: int = 60 * 10,
    ):

        order_placed = False
        start_time = time.time()
        
        linear_symbol = spot_2_linear(symbol)
        spot_bid = MarketDataStore.quote[symbol].bid
        spot_ask = MarketDataStore.quote[symbol].ask
        linear_bid = MarketDataStore.quote[linear_symbol].bid
        linear_ask = MarketDataStore.quote[linear_symbol].ask
        
        if not notional and not amount:
            raise Exception("Either 'notional' or 'amount' must be provided.")
        elif not amount:
            amount = notional / linear_ask if not close_position else notional / linear_bid
        amount = float(self._exchange.amount_to_precision(linear_symbol, amount))
        
        remain_amount = 0
        while True:
            if time.time() - start_time > wait:
                self.trade_log.info(f"Operation for {symbol} timed out after {wait} seconds. Cancelling order if exists.")
                if order_placed and res:
                    try:
                        await self._order.cancel_order(res['id'], linear_symbol)
                        self.logger.info(f"[TIME OUT] Cancelled order {res['id']} for {linear_symbol}")
                    except Exception as e:
                        self.logger.error(f"[TIME OUT] Error cancelling order for {linear_symbol}: {e}")
                return False
            
            curr_spot_bid = MarketDataStore.quote[symbol].bid
            curr_spot_ask = MarketDataStore.quote[symbol].ask
            curr_linear_bid = MarketDataStore.quote[linear_symbol].bid
            curr_linear_ask = MarketDataStore.quote[linear_symbol].ask
            
            ratio = curr_linear_bid / curr_spot_ask - 1 if not close_position else curr_linear_ask / curr_spot_bid - 1
            self.trade_log.info(f"symbol: {symbol}, ratio: {ratio}, open_ratio: {open_ratio}, spot_bid: {curr_spot_bid}, spot_ask: {curr_spot_ask}, linear_bid: {curr_linear_bid}, linear_ask: {curr_linear_ask}")
            if not order_placed:
                amount = remain_amount if remain_amount > 0 else amount
                if close_position:
                    price = (open_ratio + 1) * curr_spot_bid
                    price = float(self._exchange.price_to_precision(linear_symbol, price, mode='floor'))
                    res = await self._order.place_limit_order(
                        symbol=linear_symbol,
                        side='buy',
                        amount=amount,
                        price=price,
                        close_position=True,
                        client_order_id=self.client_id,
                    )
                else:
                    price = (open_ratio + 1) * curr_spot_ask
                    price = float(self._exchange.price_to_precision(linear_symbol, price, mode='ceil'))
                    res = await self._order.place_limit_order(
                        symbol=linear_symbol,
                        side='sell',
                        amount=amount,
                        price=price,
                        client_order_id=self.client_id,   
                    )
                if res:
                    order_placed = True
                else:
                    return False
            
            # if order_placed and res:
            #     order_res = await self._exchange.api.fetch_order(id=res['id'], symbol=linear_symbol)
            #     if order_res['status'] == 'closed':
            #         logging.info(f"Order {res['id']} for {linear_symbol} has been filled.")
            #         if res['id'] in self.order_ids:
            #             logging.info(f"[SOCKET DELAY] Symbol: {symbol}")
            #             await self.on_filled_order(order_res)
            #         return True
            
            if close_position:
                if order_placed and curr_spot_bid != spot_bid: # if curr_spot_bid changes, cancel the order
                    curr_price = (open_ratio + 1) * curr_spot_bid
                    curr_price = float(self._exchange.price_to_precision(linear_symbol, curr_price, mode='floor'))
                    if curr_price != price:
                        res = await self._order.cancel_order(res['id'], linear_symbol)
                        if res:
                            remain_amount = res.get('remaining', 0)
                            spot_bid = curr_spot_bid
                            order_placed = False
                            price = curr_price
                        else:
                            return False
                    else:
                        self.trade_log.info(f"Price: {curr_price} has not changed for {linear_symbol}.")
            else:
                if order_placed and curr_spot_ask != spot_ask:
                    curr_price = (open_ratio + 1) * curr_spot_ask
                    curr_price = float(self._exchange.price_to_precision(linear_symbol, curr_price, mode='ceil'))
                    if curr_price != price:
                        res = await self._order.cancel_order(res['id'], linear_symbol)
                        if res:
                            remain_amount = res.get('remaining', 0)
                            spot_ask = curr_spot_ask
                            order_placed = False
                            price = curr_price
                        else:
                            return False
                    else:
                        self.trade_log.info(f"Price: {curr_price} has not changed for {linear_symbol}.")
            
            # if not res:
            #     return True
            
            await asyncio.sleep(time_interval)
    
    async def order_spot(self, order: OrderResponse, symbol: str, amount: float):
        if order['side'] == 'buy': # close position of linear side
            res = await self._order.place_market_order(
                symbol=symbol,
                side='sell',
                amount=amount,
                client_order_id=self.client_id,
            )
            # logging.info(f"Place Market Order for {res['symbol']}: {amount}")
        elif order['side'] == 'sell':
            res = await self._order.place_market_order(
                symbol=symbol,
                side='buy',
                amount=amount,
                client_order_id=self.client_id,
            )
            # logging.info(f"Place Market Order for {res['symbol']}: {amount}")
        return res
        



        