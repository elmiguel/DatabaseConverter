from collections import namedtuple
from sqlalchemy import create_engine
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.sql.schema import MetaData
from database_credentials import *

# TODO: Make it so that the database source and destination type are interchangeable
#       meaning that either can be what server type the end user want


class Transport:
    def __init__(self):

        self.db_module_string = DB_MODULE_STRING
        self.db_module = DB_MODULE
        self.echo = DB_ECHO
        self.convert_from_sqlite = CONVERT_FROM_SQLITE
        self.db_sqlite_file = DB_SQLITE_FILE

        self.db_source_type = DB_SOURCE_TYPE
        self.db_source_user = DB_SOURCE_USER
        self.db_source_pass = DB_SOURCE_PASS
        self.db_source_host = DB_SOURCE_HOST
        self.db_source_port = DB_SOURCE_PORT
        self.db_source_name = DB_SOURCE_NAME

        self.db_destination_type = DB_DESTINATION_TYPE
        self.db_destination_user = DB_DESTINATION_USER
        self.db_destination_pass = DB_DESTINATION_PASS
        self.db_destination_host = DB_DESTINATION_HOST
        self.db_destination_port = DB_DESTINATION_PORT
        self.db_destination_name = DB_DESTINATION_NAME

        self.setup_sessions()

    def change_database(self, db_name, target='destination'):
        if target == 'destination':
            self.db_destination_name = db_name
        else:
            if self.convert_from_sqlite:
                self.db_sqlite_file = db_name
            else:
                self.db_source_name = db_name

        self.setup_sessions()

    def setup_sessions(self):
        if self.convert_from_sqlite:
            self.source_engine = create_engine(self.db_sqlite_file, echo=DB_ECHO)
        else:
            self.source_engine = Transport.engine(self.db_source_type, self.db_module_string, self.db_source_user,
                                                  self.db_source_pass, self.db_source_host,
                                                  self.db_source_port, self.db_source_name, self.echo, self.db_module)

        self.destination_engine = Transport.engine(self.db_destination_type, self.db_module_string,
                                                   self.db_destination_user, self.db_destination_pass,
                                                   self.db_destination_host, self.db_destination_port,
                                                   self.db_destination_name, self.echo, self.db_module)

        source_session = sessionmaker(bind=self.source_engine)
        destination_session = sessionmaker(bind=self.destination_engine)

        # source_session.configure(bind=self.source_engine)
        # destination_session.configure(bind=self.destination_engine)

        db_source = source_session()
        db_destination = destination_session()

        self.sessions = namedtuple('sessions', 'source, destination')
        self.sessions.source = db_source
        self.sessions.destination = db_destination

    @staticmethod
    def engine(db_type, module, user, passwd, host, port, database, echo, db_module):
        # if default, the remove it as it will cause connections issues fore mysql ... =/
        if db_type == 'mysql' and port == 3306:
            port = ''
        else:
            port = ':{port}'.format(port=port)

        return create_engine("{db_type}+{module}://{user}:{passwd}@{host}{port}/{db}".format(
            db_type=db_type,
            module=module,
            user=user,
            passwd=passwd,
            host=host,
            port=port,
            db=database
        ), echo=echo, module=db_module)

    def run(self):
        # Source reflection
        source_meta = MetaData()
        source_meta.reflect(bind=self.source_engine)
        source_tables = source_meta.tables

        source_table_names = [k for k, v in source_tables.items()]

        # Destination Binding
        destination_meta = MetaData(bind=self.destination_engine)
        for name, table in source_tables.items():
            table.metadata = destination_meta

        # pp(source_meta.tables)

        # Drop table for testing purposes
        # destination_meta.drop_all(transport.destination_engine)

        # Begin migration
        source_meta.create_all(self.destination_engine)

        source_data = {table: self.sessions.source.query(source_tables[table]).all() for table in source_table_names}

        # pp(source_data)

        for table in source_table_names:
            for row in source_data[table]:
                self.sessions.destination.execute(source_tables[table].insert(row))

        self.sessions.destination.commit()

