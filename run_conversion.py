from db_converter import Transport

# Initialize the transport
transport = Transport()

# Run the migration
transport.run()

# Change the destination db name and setup the sessions, then run the job again
# transport.change_database('test')
# transport.run()