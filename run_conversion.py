from sqlalchemy.sql.schema import MetaData
from converter_config import Transport
from sqlalchemy.engine import reflection
from sqlalchemy.ext.automap import automap_base
from pprint import pprint as pp


# Initialize the transport
transport = Transport()

# Source reflection
source_meta = MetaData()
source_meta.reflect(bind=transport.source_engine)

# Destination reflection
destination_meta = MetaData()
destination_meta.reflect(bind=transport.destination_engine)

# Use Inspection grab the table names to avoid having to manually create Models
inspection_source = reflection.Inspector.from_engine(transport.source_engine)
inspection_destination = reflection.Inspector.from_engine(transport.destination_engine)

# Store the table names: in this case the destination is empty
source_table_names = inspection_source.get_table_names()
destination_table_names = inspection_destination.get_table_names()

source_table_columns = {table: inspection_source.get_columns(table) for table in source_table_names}
# pp(source_table_columns)


# Create automapped 'Models' from the source and transport them to the destination
Base = automap_base(metadata=source_meta)
Base.prepare()

# print(Base.classes[source_table_names[0]])
# for model in Base.classes:
#     pp(model)
# print(Base.classes['users'])


# Drop table for testing purposes
# destination_meta.drop_all(transport.destination_engine)

# Begin migration
source_meta.create_all(transport.destination_engine)

print(destination_meta.tables['users'].insert().from_select(
        names=[c['name'] for c in source_table_columns['users']],
        select=source_meta.tables['users'].select()))

for table in source_table_names:
    # model = Base.classes[table]
    # pp(model)
    # data = transport.sessions.source.query(model).limit(10)
    transport.destination_engine.execute(destination_meta.tables[table].insert().from_select(
        names=[c['name'] for c in source_table_columns[table]],
        select=source_meta.tables[table].select()
    ), autocommit=True)

