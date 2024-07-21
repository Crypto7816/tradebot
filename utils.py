import asyncio
import logging
import json

import aiohttp
import websockets


from typing import Literal, Dict


async def get_listen_key(base_url: str, api_key: str):
    headers = {'X-MBX-APIKEY': api_key}
    async with aiohttp.ClientSession() as session:
        async with session.post(base_url, headers=headers) as response:
            data = await response.json()
            return data['listenKey']

async def keep_alive_listen_key(base_url: str, api_key: str, listen_key: str):
    headers = {'X-MBX-APIKEY': api_key}
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                logging.info('Keep alive listen key...')
                await asyncio.sleep(20 * 60)
                res = await session.put(f'{base_url}?listenKey={listen_key}', headers=headers)
                logging.info(f"Keep alive listen key status: {res.status}")
                if res.status != 200:
                    listen_key = await get_listen_key(base_url, api_key)
                else:
                    data = await res.json()
                    logging.info(f"Keep alive listen key: {data['listenKey']}")
        except Exception as e:
            logging.error(f"Error keeping alive listen key: {e}")
                
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
    
    
    asyncio.create_task(keep_alive_listen_key(base_url, api_key, listen_key))
    
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
    


