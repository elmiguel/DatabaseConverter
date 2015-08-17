from db_converter import Transport, Settings

# Setup the settings file and load it: Empty will use the default.ini file!
test = Settings()
# Initialize the transport
transport = Transport(test)
# Run the migration
transport.run()
print('default (sqlite_to_mysql): complete!')

# Fails! Worked in Mac Failed on Linux
mysql_to_mysql = Settings('mysql_to_mysql.ini')
transport = Transport(mysql_to_mysql)
transport.run()
print('mysql_to_mysql: complete!')

mysql_to_sqlite = Settings('mysql_to_sqlite.ini')
transport = Transport(mysql_to_sqlite)
transport.run()
print('mysql_to_sqlite: complete!')

sqlite_to_sqlite = Settings('sqlite_to_sqlite.ini')
transport = Transport(sqlite_to_sqlite)
transport.run()
print('sqlite_to_sqlite: complete!')
