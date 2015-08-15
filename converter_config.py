from collections import namedtuple
from sqlalchemy import create_engine
from sqlalchemy.orm.session import sessionmaker
from database_credentials import *


class Transport:
    def __init__(self, convert_from_sqlite=True):

        if convert_from_sqlite:
            self.source_engine = create_engine(DB_SQLITE_FILE, echo=DB_ECHO)
        else:
            self.source_engine = Transport.engine(DB_SOURCE_ENGINE, DB_MODULE_STRING, DB_SOURCE_USER,
                                                  DB_SOURCE_PASS, DB_SOURCE_HOST,
                                                  DB_SOURCE_PORT, DB_SOURCE_NAME)

        self.destination_engine = Transport.engine(DB_DESTINATION_ENGINE, DB_MODULE_STRING, DB_DESTINATION_USER,
                                                   DB_DESTINATION_PASS, DB_DESTINATION_HOST,
                                                   DB_DESTINATION_PORT, DB_DESTINATION_NAME)

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
    def engine(db_type, module, user, passwd, host, port, database):
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
        ), echo=DB_ECHO, module=DB_MODULE)

