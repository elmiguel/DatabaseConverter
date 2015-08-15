from converter_config import Transport

# Initialize the transport
transport = Transport()

# Run the migration
transport.run()

# Change the destination db name and setup the sessions, then run the job again
transport.db_destination_name = 'test'
transport.setup_sessions()
transport.run()