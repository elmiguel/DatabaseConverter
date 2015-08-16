from db_converter import Transport, Settings

# Setup the settings file and load it: Empty will use the default.ini file!
# test = Settings()

# Initialize the transport
# transport = Transport(test)

# Run the migration
# transport.run()

# bba_data = Settings('sqlite_to_mysql_bba_data.ini')
# transport = Transport(bba_data)
# transport.run()

# test_dest = Settings('sqlite_to_sqlite.ini')
# transport = Transport(test_dest)
# transport.run()

test_dest = Settings('mysql_to_sqlite.ini')
transport = Transport(test_dest)
transport.run()