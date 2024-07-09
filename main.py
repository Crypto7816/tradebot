import ssl
import asyncio


import uvloop
import nats

from bot import TradingBot
from manager import NatsManager


async def main():
    ssl_ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
    ssl_ctx.load_cert_chain(certfile='./keys/server-cert.pem',
                            keyfile='./keys/server-key.pem')
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    nc = await nats.connect("nats://104.194.152.27:4222", tls=ssl_ctx)
    
    symbols = ['AEVO/USDT']
    
    bot = TradingBot(symbols)
    nats_manager = NatsManager(nc)
    
    
    await nats_manager.subscribe()


if __name__ == '__main__':
    uvloop.install()
    asyncio.run(main())