import asyncio
import json
import random
import string
import time


import aiohttp
import websockets


from typing import Literal, Dict
from decimal import Decimal, ROUND_HALF_UP, ROUND_CEILING, ROUND_FLOOR


from entity import Context
from entity import log_register


logger = log_register.get_logger('utils', level='INFO')


async def get_listen_key(base_url: str, api_key: str):
    headers = {'X-MBX-APIKEY': api_key}
    async with aiohttp.ClientSession() as session:
        async with session.post(base_url, headers=headers) as response:
            data = await response.json()
            return data['listenKey']

async def keep_alive_listen_key(base_url: str, api_key: str, listen_key: str, typ: Literal['spot', 'linear', 'inverse']):
    headers = {'X-MBX-APIKEY': api_key}
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                logger.info(f'Keep alive {typ} listen key...')
                async with session.put(f'{base_url}?listenKey={listen_key}', headers=headers) as res:
                    logger.info(f"Keep alive listen key status: {res.status}")
                    if res.status != 200:
                        listen_key = await get_listen_key(base_url, api_key)
                    else:
                        data = await res.json()
                        logger.info(f"Keep alive {typ} listen key: {data.get('listenKey', listen_key)}")
                    await asyncio.sleep(60 * 20)
        except Exception as e:
            logger.error(f"Error keeping alive {typ} listen key: {e}")
            
                  
async def user_data_stream(typ: Literal['spot', 'linear', 'inverse'], api_key:str, queue: asyncio.Queue):
    if typ == 'spot':
        base_url = 'https://api.binance.com/api/v3/userDataStream'
        stream_url = 'wss://stream.binance.com:9443/ws/'
    elif typ == 'linear':
        base_url = 'https://fapi.binance.com/fapi/v1/listenKey'
        stream_url = 'wss://fstream.binance.com/ws/'
    elif typ == 'inverse':
        base_url = 'https://dapi.binance.com/dapi/v1/listenKey'
        stream_url = 'wss://dstream.binance.com/ws/'
    
    listen_key = await get_listen_key(base_url, api_key)
    ws_url = f'{stream_url}{listen_key}'
    
    
    asyncio.create_task(keep_alive_listen_key(base_url, api_key, listen_key, typ))
    # asyncio.create_task(keep_binance_listenkey_alive(api_key, listen_key))
    
    async with websockets.connect(ws_url) as ws:
        while True:
            message = await ws.recv()
            res = json.loads(message)
            await queue.put(res)


def parse_order_status(status: str):
    statuses: Dict[str, str] = {
        'NEW': 'new',
        'PARTIALLY_FILLED': 'partially_filled',
        'FILLED': 'filled',
        'CANCELED': 'canceled',
        'EXPIRED': 'expired',
        'EXPIRED_IN_MATCH': 'expired' 
    }
    return statuses.get(status, None)

def parse_symbol(symbol: str, typ: Literal['spot', 'linear']):
    if typ not in ['spot', 'linear', 'inverse']:
            raise ValueError(f"Unsupported market type: {typ}")
    if typ == 'spot':
        return symbol.replace('USDT', '/USDT')
    elif typ == 'linear':
        return symbol.replace('USDT', '/USDT:USDT')

def parse_account_update(res: Dict, typ: Literal['spot', 'future'], context: Context):
    base_asset = ['USDT', 'BTC', 'ETH', 'BNB', 'USDC', 'FDUSD']
    if typ == 'future':
        balance = res['a']['B']
        for data in balance:
            if data['a'] in base_asset:
                context.futures_account[data['a']] = float(data['wb'])
    elif typ == 'spot':
        balance = res['B']
        for data in balance:
            if data['a'] in base_asset:
                context.spot_account[data['a']] = float(data['f'])

def spot_2_linear(symbol: str):
    if symbol.endswith(':USDT'):
        return symbol
    else:
        return symbol + ':USDT'
            
def linear_2_spot(symbol: str):
    if symbol.endswith(':USDT'):
        return symbol[:-5]
    else:
        return symbol

def is_linear(symbol: str):
    return ':' in symbol

def is_spot(symbol: str):
    return ':' not in symbol

def generate_client_order_id(prefix='x-'):
    timestamp = int(time.time() * 1000)
    random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
    combined = f"{random_part}{timestamp}"
    while len(combined) < 32:
        combined += random.choice(string.ascii_lowercase + string.digits)
    combined = combined[:32]
    return f"{prefix}{combined}"

def price_to_precision(symbol: str, price: float, mode: Literal['round', 'ceil', 'floor'], market: Dict):
    market = market[symbol]
    price = Decimal(str(price))
    precision = Decimal(str(market['precision']['price']))
    
    if mode == 'round':
        return price.quantize(precision, rounding=ROUND_HALF_UP)
    elif mode == 'ceil':
        return price.quantize(precision, rounding=ROUND_CEILING)
    elif mode == 'floor':
        return price.quantize(precision, rounding=ROUND_FLOOR)

def amount_to_precision(symbol: str, amount: float, mode: Literal['round', 'ceil', 'floor'], market: Dict):
    market = market[symbol]
    amount = Decimal(str(amount))
    precision = Decimal(str(market['precision']['amount']))
    
    if mode == 'round':
        return amount.quantize(precision, rounding=ROUND_HALF_UP)
    elif mode == 'ceil':
        return amount.quantize(precision, rounding=ROUND_CEILING)
    elif mode == 'floor':
        return amount.quantize(precision, rounding=ROUND_FLOOR)
    
    
   


