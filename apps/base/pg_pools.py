import momoko

from tornado import gen

from apps.base.ioloop import IOLoop
from apps.options import settings

__all__ = ('Connection',)


class _Pool(object):

    def __init__(self, settings, pool_size=10):
        self.settings = settings
        self.pool_size = pool_size
        self.pool = None

    @gen.coroutine
    def init(self):
        if not self.settings:
            return self

        ioloop = IOLoop.instance()
        self.pool = momoko.Pool(
            dsn='dbname={dbname} user={username} password={password} host={host} port={port}'.format(
                **self.settings
            ),
            size=self.pool_size,
            ioloop=ioloop,
        )
        ioloop.add_future(self.pool.connect(), lambda f: ioloop.stop())
        ioloop.start()
        return self


Connection = _Pool(dict(
    dbname=settings.POSTGRESQL_DB,
    username=settings.POSTGRESQL_USER,
    password=settings.POSTGRESQL_PASS,
    host=settings.POSTGRESQL_HOST,
    port=settings.POSTGRESQL_PORT,
))
Connection.init()
