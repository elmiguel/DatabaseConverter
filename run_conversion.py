from sqlalchemy.sql.schema import MetaData
from converter_config import Transport
from pprint import pprint as pp


# Initialize the transport
transport = Transport()

# Source reflection
source_meta = MetaData()
source_meta.reflect(bind=transport.source_engine)
source_tables = source_meta.tables

source_table_names = [k for k, v in source_tables.items()]

# Destination Binding
destination_meta = MetaData(bind=transport.destination_engine)
for name, table in source_tables.items():
    table.metadata = destination_meta

# pp(source_meta.tables)


# Drop table for testing purposes
# destination_meta.drop_all(transport.destination_engine)

# Begin migration
source_meta.create_all(transport.destination_engine)

source_data = {table: transport.sessions.source.query(source_tables[table]).all() for table in source_table_names}

# pp(source_data)

for table in source_table_names:
    for row in source_data[table]:
        transport.sessions.destination.execute(source_tables[table].insert(row))

transport.sessions.destination.commit()