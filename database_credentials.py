# The engine types: mysql, mssql, sqlite, oracle, postgresql
DB_SOURCE_ENGINE = 'sqlite'
DB_DESTINATION_ENGINE = 'mysql'

# The module types: pyodbc, pypyodbc, psycopg2, pg8000, mysqldb, mysqlconnector, oursql, pymssql to name a few
DB_MODULE = 'pyodbc'
DB_ECHO = True

# sqlite://<nohostname>/<path>
# where <path> is relative:
# sqlite:///foo.db
# Unix/Mac - 4 initial slashes in total
# sqlite:////absolute/path/to/foo.db
# Windows
# sqlite:///C:\\path\\to\\foo.db
# Windows alternative using raw string
# sqlite:///C:\path\to\foo.db

DB_SQLITE_FILE = 'sqlite:////home/elmiguel/Documents/PycharmProjects/BbAdminApp/bba_app.db'


DB_SOURCE_HOST = ''
DB_SOURCE_USER = ''
DB_SOURCE_PASS = ''
DB_SOURCE_PORT = ''
DB_SOURCE_NAME = ''

DB_DESTINATION_HOST = 'localhost'
DB_DESTINATION_USER = 'root'
DB_DESTINATION_PASS = ''
DB_DESTINATION_PORT = 3306
DB_DESTINATION_NAME = 'bba_app'
