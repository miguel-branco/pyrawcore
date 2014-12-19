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
import base64
import collections
import cPickle

import cloud
import matplotlib
import numpy
import pandas
import prettytable


def decode_func(f):
    return cPickle.loads(base64.b64decode(f))

def encode_func(f):
    return base64.b64encode(cloud.serialization.cloudpickle.dumps(f))


class Table(object):

    def __init__(self, columns_added=[], columns_hidden=[]):
        self._columns_added = collections.OrderedDict(columns_added)
        self._columns_hidden = set(columns_hidden)

    def _encode_columns_added(self):
        return [(name, encode_func(func)) for name, func in self._columns_added.items()]

    def _encode_columns_hidden(self):
        return list(self._columns_hidden)

    @staticmethod
    def _decode_columns_added(payload):
        return [(name, decode_func(func)) for name, func in payload['columns_added']]

    @staticmethod
    def _decode_columns_hidden(payload):
        return payload['columns_hidden']

    def _new_tuple_from_dict(self, values_dict):
        attrs = dict()
        for name, value in values_dict.items():         # Build record
            attrs[name] = value
        for name, func in self._columns_added.items():  # Add extra columns
            attrs[name] = func(attrs)
        for name in self._columns_hidden:               # Remove hidden columns
            del attrs[name]
        return attrs

    def _new_tuple(self, schema, values):
        attrs = collections.OrderedDict()
        for name, value in zip(schema, values):         # Build record
            attrs[name] = value
        for name, func in self._columns_added.items():  # Add extra columns
            attrs[name] = func(attrs)
        for name in self._columns_hidden:               # Remove hidden columns
            del attrs[name]
        return attrs

    def __iter__(self):
        return self._get_iterator()

    def __getitem__(self, key):
        if isinstance(key, (int, long)):
            return self._get_key(key)
        elif isinstance(key, slice):
            return self._get_slice(key)
        elif isinstance(key, (str, unicode)):
            return self.__get_attribute(key)
        raise ValueError('key is not a row number, row range or column name')

    def __setitem__(self, key, value):
        if isinstance(key, (str, unicode)) and hasattr(value, '__call__'):
            self._columns_added[key] = value

    def __delitem__(self, key):
        if isinstance(key, (str, unicode)):
            if key in self._columns_added:
                del self._columns_added[key]
            else:
                self._columns_hidden.add(key)

    def keys(self):
        return self._get_keys()

    def values(self):
        return [self.__get_attribute(key) for key in self._get_keys()]

    def items(self):
        return zip(self.keys(), self.values())

    def __get_added_attribute(self, name):
        for row in iter(self):
            yield row[name]

    def __get_attribute(self, name):
        if name in self._columns_hidden:
            raise KeyError('column %s hidden' % name)

        if name in self._columns_added:
            # This is a slow but needed since the user's code can depend on other columns.
            return self.__get_added_attribute(name)
        else:
            return self._get_column(name)

    def _repr_html_(self):
        """Pretty print for IPython Notebook.
        """
        pretty = None
        n = 0
        for row in self._get_iterator():
            if pretty is None:
                pretty = prettytable.PrettyTable(row.keys())            
            if n < 10:
                pretty.add_row(row.values())
            n += 1
            if n > 10:
                break

        if not n:
            return '<span style="font-style:italic;text-align:center;">No rows found</span>'

        html = pretty.get_html_string()
        if n > 10:
            html += '<span style="font-style:italic;text-align:center;">... result truncated.</span>'
        return html

    def pandas_dataframe(self):
        """Return a Pandas DataFrame.
        """
        data = {}
        for name in self._get_keys():
            data[name] = list(self.__get_attribute(name))
        return pandas.DataFrame(data)

    def plot(self, *args, **kwargs):
        """Return a plot (from Pandas Dataframe).
        """
        return self.pandas_dataframe().plot(*args, **kwargs)
