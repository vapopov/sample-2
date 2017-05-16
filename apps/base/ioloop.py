import asyncio
import uvloop
from tornado.platform.asyncio import AsyncIOLoop


__all__ = ('IOLoop',)


class IOLoop(AsyncIOLoop):

    def initialize(self, **kwargs):
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        super().initialize(**kwargs)
        asyncio.set_event_loop(self.asyncio_loop)
