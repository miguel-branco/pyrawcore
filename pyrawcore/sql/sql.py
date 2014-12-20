# -*- coding: utf-8 -*-
#
# The MIT License (MIT)
#
#                           Copyright (c) 2014
#       Data Intensive Applications and Systems laboratory (DIAS)
#                École Polytechnique Fédérale de Lausanne
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

# TODO: If table contains > MAX fields, create table with columns referenced in query only.
#       Hash set of columns referenced in query and used that as the PostgreSQL table id,
#       to reuse the same table in future queries that reference the same subset of columns.
#       Disadvantage of hashing is that it disallows reusing tables with all needed columns
#       but which also have a few extra.

import collections
import json
import os
import uuid

import pandas
import psycopg2
import psycopg2.extras
from ..core import get_option, load, Table, is_table


resource_path = get_option('sql', 'resource_path')
if not resource_path:
    import tempfile
    resource_path = os.path.realpath(tempfile.gettempdir())


class SQL(Table):
    
    CHUNK_SIZE = 100000

    TypesMap = {
        int: 'INTEGER',
        long: 'BIGINT',
        float: 'DOUBLE PRECISION',
        str: 'TEXT',
        unicode: 'TEXT',
        bool: 'BOOLEAN'
    }

    def __init__(self, sql, tables, columns_added=[], columns_hidden=[]):
        super(SQL, self).__init__(columns_added=columns_added, columns_hidden=columns_hidden)
        # TODO: Validate sql statement
        for name, resource in tables.items():
            if not isinstance(name, (str, unicode)):
                raise ValueError('table name is not a str or unicode')
            if not hasattr(resource, 'to_json'):
                raise ValueError('table resource is not serializable')
        self.sql = sql
        self.tables = tables

        self.__connect()
        self.__create_schema()
        self.__create_tables()

    def __del__(self):
        self.__drop_schema()
        self.__disconnect()

    @staticmethod
    def from_json(payload):
        return SQL(
            payload['sql'],
            {table_name: load(table_json) for table_name, table_json in self.tables.items()},
            columns_added=Table._decode_columns_added(payload),
            columns_hidden=Table._decode_columns_hidden(payload))

    def to_json(self):
        return dict(
            name='sql',
            payload=dict(
                sql=self.sql,
                tables={table_name: table_resource.to_json() for table_name, table_resource in self.tables.items()},
                columns_added=self._encode_columns_added(),
                columns_hidden=self._encode_columns_hidden()))

    def __connect(self):
        self.__conn = psycopg2.connect(get_option('sql', 'connection_string'))
        self.__conn.set_isolation_level(0)

    def __disconnect(self):
        self.__conn.close()

    def __create_schema(self):
        self.__schema = 'schema_%s' % str(uuid.uuid4()).replace('-', '')
        with self.__conn.cursor() as cur:
            cur.execute("CREATE SCHEMA %s" % self.__schema)
            cur.execute("SET search_path to %s, public" % self.__schema)

    def __drop_schema(self):
        with self.__conn.cursor() as cur:
            cur.execute("DROP SCHEMA %s CASCADE" % self.__schema)

    def __get_schema(self, name, resource):
        # TODO: Improve schema detection; e.g. what if first row contains None fields?
        schema = is_table(resource)
        if not schema:
            raise RuntimeError('%s is not a table' % name)
        return schema

    def __add_table_stmt(self, name, schema, options):
        sql = "CREATE FOREIGN TABLE %s.%s (" % (self.__schema, name)
        for column_name, column_type in schema.items():
            sql += '"%s" %s,' % (column_name, SQL.TypesMap[column_type])
        sql = sql[:-1]
        sql += ") SERVER rawfdw OPTIONS ("
        for k, v in options.items():
            sql += """"%s" '%s',""" % (k, v)
        sql = sql[:-1]
        sql += ")"
        return sql

    def __create_tables(self):
        with self.__conn.cursor() as cur:
            for name, resource in self.tables.items():
                schema = self.__get_schema(name, resource)

                path = os.path.join(resource_path, str(uuid.uuid4()))
                with open(path, 'w') as f:
                    json.dump(resource.to_json(), f)
                
                # If the PostgreSQL instance is not running in the current account,
                # make the resource file readable by all users.
                #if not postgres_running_in_user_account:
                #    os.chmod(f.name, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

                cur.execute(self.__add_table_stmt(name, schema, dict(path=path)))

    def _get_iterator(self):
        with self.__conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(self.sql)
            rows = cur.fetchmany(SQL.CHUNK_SIZE)
            while rows:
                for row in rows:
                    yield self._new_tuple_from_dict(row)
                rows = cur.fetchmany(SQL.CHUNK_SIZE)

    def _get_keys():
        raise NotImplementedError('SQL._get_keys()')

    def _get_key(self, key):
        with self.__conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM (%s) AS t LIMIT 1 OFFSET %d" % (self.sql, key))
            row = cur.fetchone()
            if not row:
                raise IndexError('index out of range')
            return self._new_tuple_from_dict(row)

    def _get_slice(self, slice):
        if slice.step:
            raise NotImplementedError('slice step not supported')

        start, stop = slice.start, slice.stop

        if not start:
            start = 0

        if stop is not None and stop < start:
            raise NotImplementedError('slice backward not supported')            

        with self.__conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if stop is not None:
                cur.execute("SELECT * FROM (%s) AS t LIMIT %d OFFSET %d" % (self.sql, stop - start, start))
            else:
                cur.execute("SELECT * FROM (%s) AS t OFFSET %d" % (self.sql, start))

            rows = cur.fetchmany(SQL.CHUNK_SIZE)
            while rows:
                for row in rows:
                    yield self._new_tuple_from_dict(row)
                rows = cur.fetchmany(SQL.CHUNK_SIZE)

    def _get_column(self, name):
        raise NotImplementedError('SQL._get_column()')

    # def _get_column(self, name):
    #     with self.__conn.cursor() as cur:
    #         cur.execute("SELECT %s FROM (%s) AS t" % (name, self.sql))
    #
    #         rows = cur.fetchmany(SQL.CHUNK_SIZE)
    #         while rows:
    #             for row in rows:
    #                 yield row[0]
    #             rows = cur.fetchmany(SQL.CHUNK_SIZE)

    def pandas_dataframe(self):
        # Overridding default implementation
        data = {}
        for row in iter(self):
            if not data:
                for key in row:
                    data.setdefault(key, [])
            for key in row:
                data[key].append(row[key])
        return pandas.DataFrame(data)
