from tornado import options


options.define('PORT', type=str, default='8080', help='Application Port')
options.define('DEBUG', type=bool, default=True, help='Debug mode')
options.define('AUTORELOAD', type=bool, default=False, help='Restart applicaiton when source is modified')

options.define('RABBITMQ_URL', type=str, default='amqp://guest:guest@localhost/simplesurance', help='RMQ url')

options.define('POSTGRESQL_DB', type=str, default='postgres', help='Postgresql database name')
options.define('POSTGRESQL_USER', type=str, default='zion', help='Postgresql database user')
options.define('POSTGRESQL_PASS', type=str, default=None, help='Postgresql database pass')
options.define('POSTGRESQL_HOST', type=str, default='localhost', help='Postgresql database host')
options.define('POSTGRESQL_PORT', type=int, default=5432, help='Postgresql database port')

options.parse_command_line()

settings = options.options
