from io import StringIO
from os import environ
import psycopg2


class Postgres(object):
    __database: str
    __conn = None
    __timeout: int = 30

    def __init__(self, host: str = None, port: int = None, user: str = None, password: str = None, database: str = None,
                 timeout: int = 30):
        """
        :param host: Server Host
        :param port: Server Port
        :param user: Server User
        :param password: Server Password
        :param database: Database name
        :param timeout: Connection timeout
        """

        self.__host = environ.get('POSTGRE_HOST', host)
        self.__port = environ.get('POSTGRE_PORT', port)
        self.__user = environ.get('POSTGRE_USERNAME', user)
        self.__password = environ.get('POSTGRE_PASSWORD', password)

        assert self.__host, 'host must be informed on instance or by environment variable'
        assert self.__port, 'port must be informed on instance or by environment variable'
        assert self.__user, 'username must be informed on instance or by environment variable'
        assert self.__password, 'password must be informed on instance or by environment variable'

        self.__database = database
        self.__timeout = timeout
        self.__connect()

    def __connect(self):

        self.__conn = psycopg2.connect(host=self.__host,
                                       port=self.__port,
                                       database=self.__database,
                                       user=self.__user,
                                       password=self.__password,
                                       connect_timeout=self.__timeout)

    def insert(self, query: str, params: list) -> int:
        """
        Execute an insert operation on database
        :param query: Write statement
        :param params: List of tuples with data to be inserted
        :return: Number of inserted rows.
        """
        try:
            if self.__conn.close:
                self.__connect()
            # create a new cursor
            cur = self.__conn.cursor()
            # execute the INSERT statement
            rows_count = cur.executemany(query, params)
            # commit the changes to the database
            self.__conn.commit()
            # close communication with the database
            cur.close()
            return rows_count
        except (Exception, psycopg2.DatabaseError) as error:
            self.__conn.rollback()
            raise error
        finally:
            if self.__conn is not None:
                self.__conn.close()

    def bulk_insert(self, table: str, data: str, columns: tuple, sep: str = '\t', null_value: str = 'None') -> None:
        """
        Function that apply a bulk insert at database
        :param table: table name where data will be insert
        :param data: Multiline string with data. Break line must be \n.
        :param columns: Columns to which data refers
        :param sep: Specifies the character that separates columns within each row (line) of the file.
        :param null_value: Specifies the string that represents a null value
        """
        try:
            if self.__conn.close:
                self.__connect()
            # create a new cursor
            cur = self.__conn.cursor()
            # execute the INSERT statement
            file = StringIO(data)

            if float(psycopg2.__version__[:3]) >= 2.9:
                sql = "COPY {}({}) FROM STDIN  WITH NULL AS '{}' DELIMITER as '{}'".format(
                    table, ','.join(columns), null_value, sep
                )

                cur.copy_expert(sql=sql, file=file)
            else:
                cur.copy_from(file=file, table=table, sep=sep, columns=columns, null=null_value)

            self.__conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            self.__conn.rollback()
            raise error
        finally:
            if self.__conn is not None:
                self.__conn.close()

    def execute_query(self, query: str, params: tuple = None) -> None:
        """
        Execute a statement with no return.
        :param query: Query to be executed
        :param params: Query params

        """
        try:
            if self.__conn.close:
                self.__connect()
            cur = self.__conn.cursor()
            cur.execute(query, params)
            self.__conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            self.__conn.rollback()
            raise error
        finally:
            if self.__conn is not None:
                self.__conn.close()

    def read(self, query: str, params: tuple = None, to_dict: bool = False) -> list:
        """
        Retrieve data from database based on informed query
        :param query: Read statement
        :param params: Params of read statement (%s must be used)
        :param to_dict: If true, change function returns to a list o dict
        :return: List of tuple or dict
        """
        try:
            if self.__conn.close:
                self.__connect()
            cur = self.__conn.cursor()
            rows_count = cur.execute(query, params)
            if rows_count == 0:
                cur.close()
                return []
            rows = cur.fetchall()
            cur.close()
            if to_dict:
                column_names = [desc[0] for desc in cur.description]
                return [dict(zip(column_names, x)) for x in rows]
            return rows
        except (Exception, psycopg2.DatabaseError) as error:
            raise error
        finally:
            if self.__conn is not None:
                self.__conn.close()
