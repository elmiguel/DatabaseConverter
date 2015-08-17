from pprint import pprint
from collections import namedtuple
from sqlalchemy import create_engine
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.sql.schema import MetaData
from sqlalchemy.exc import StatementError
import configparser
import importlib as imp

# TODO: Make it so that the database source and destination type are interchangeable
# meaning that either can be what server type the end user want
import sys


class Settings:
    def __init__(self, config_file=None):
        if not config_file:
            config_file = './default.ini'

        self.config = configparser.ConfigParser()
        self.config.sections()
        self.config.read(config_file)

        self.echo = True if self.config['main']['echo'] == 'True' else False
        self.convert_from_sqlite = True if self.config['main']['convert_from_sqlite'] == 'True' else False
        self.convert_to_sqlite = True if self.config['main']['convert_to_sqlite'] == 'True' else False

        if not self.convert_from_sqlite:
            self.db_source_type = self.config['source']['type']
            self.db_source_module = imp.import_module(self.config['source']['module'])
            self.db_source_module_string = self.config['source']['module']
            self.db_source_user = self.config['source']['user']
            self.db_source_pass = self.config['source']['pass']
            self.db_source_host = self.config['source']['host']
            self.db_source_port = self.config['source']['port']
            self.db_source_name = self.config['source']['name']
        else:
            self.db_source_sqlite_file = self.config['source']['sqlite_file'].replace("'", '')

        if not self.convert_to_sqlite:
            self.db_destination_type = self.config['destination']['type']
            self.db_destination_module = imp.import_module(self.config['destination']['module'])
            self.db_destination_module_string = self.config['destination']['module']
            self.db_destination_user = self.config['destination']['user']
            self.db_destination_pass = self.config['destination']['pass']
            self.db_destination_host = self.config['destination']['host']
            self.db_destination_port = self.config['destination']['port']
            self.db_destination_name = self.config['destination']['name']
        else:
            self.db_destination_sqlite_file = self.config['destination']['sqlite_file'].replace("'", '')


        # allow to exclude certain from transfer
        self.exclude_data = {}


class Transport:
    def __init__(self, settings):

        self.settings = settings
        self.source_engine = None
        self.destination_engine = None
        self.sessions = None
        self.setup_sessions()

    def change_database(self, db_name, target='destination'):
        if target == 'destination':
            self.settings.db_destination_name = db_name
        else:
            if self.settings.convert_from_sqlite:
                self.settings.db_sqlite_file = db_name
            else:
                self.settings.db_source_name = db_name

        self.setup_sessions()

    def setup_sessions(self):
        if self.settings.convert_from_sqlite:
            self.source_engine = create_engine(self.settings.db_source_sqlite_file, echo=self.settings.echo)
        else:
            self.source_engine = Transport.engine(self.settings.db_source_type, self.settings.db_source_module_string,
                                                  self.settings.db_source_user, self.settings.db_source_pass,
                                                  self.settings.db_source_host, self.settings.db_source_port,
                                                  self.settings.db_source_name, self.settings.echo,
                                                  self.settings.db_source_module)

        if self.settings.convert_to_sqlite:
            self.destination_engine = create_engine(self.settings.db_destination_sqlite_file, echo=self.settings.echo)
        else:
            self.destination_engine = Transport.engine(self.settings.db_destination_type,
                                                       self.settings.db_destination_module_string,
                                                       self.settings.db_destination_user,
                                                       self.settings.db_destination_pass,
                                                       self.settings.db_destination_host,
                                                       self.settings.db_destination_port,
                                                       self.settings.db_destination_name, self.settings.echo,
                                                       self.settings.db_destination_module)

        source_session = sessionmaker(bind=self.source_engine)
        destination_session = sessionmaker(bind=self.destination_engine)

        db_source = source_session()
        db_destination = destination_session()

        self.sessions = namedtuple('sessions', 'source, destination')
        self.sessions.source = db_source
        self.sessions.destination = db_destination

    @staticmethod
    def engine(db_type, module, user, passwd, host, port, database, echo, db_module):
        # if default, then remove it as it will cause connections issues for mysql ... =/
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
            if name in self.settings.exclude_data.keys():
                table.__mapper_args__ = {'exclude_properties': self.settings.exclude_data[name]}

        # Drop table for testing purposes
        # destination_meta.drop_all(self.destination_engine)
        for table in source_table_names:
            self.sessions.destination.execute('DROP TABLE {table};'.format(table=table))
            self.sessions.destination.commit()
            print('DROPPED TABLE {table}'.format(table=table))

        # Begin migration
        source_meta.create_all(self.destination_engine)

        source_data = {table: self.sessions.source.query(source_tables[table]).all() for table in source_table_names}

        for table in source_table_names:
            print("Migrating:", table)
            # if table in self.settings.exclude_data.keys():
            #     pprint(source_tables[table].__mapper_args__)
            #     exit(1)
            for row in source_data[table]:
                try:
                    self.sessions.destination.execute(source_tables[table].insert(row))
                except StatementError:
                    print('Bad data in table: ', table, 'row data:\n', row[0], 'Error:', sys.exc_info()[0])
            print('Data for:', table, 'added to the queue..')

        self.sessions.destination.commit()
        print('Migration Complete!')
