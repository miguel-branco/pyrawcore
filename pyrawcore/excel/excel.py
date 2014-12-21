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
import collections
import os

import pandas
from ..core import get_option, Table


def get_chunk_schema(chunk):
    schema = collections.OrderedDict()
    for name in chunk.columns:
        schema[str(name)] = chunk[name].dtype
    return schema


base_path = get_option('files', 'base_path')


class Excel(Table):

    class Column(object):

        def __init__(self, parent, column):
            self.parent = parent
            self.column = column

        def __iter__(self):
            data = pandas.read_excel(self.parent._get_path(), parse_cols=[self.column], **self.parent.args)
            for row in data.values:
                yield row[0]

        def __get_key(self, key):
            if key < 0:
                raise NotImplementedError('index backward not support')

            data = pandas.read_excel(self.parent._get_path(), parse_cols=[self.column], **self.parent.args)
            return data.values[key][0]

        def __get_slice(self, slice):
            if slice.step:
                raise NotImplementedError('slice step not supported')

            start, stop = slice.start, slice.stop

            if not start:
                start = 0

            if stop is not None and stop < start:
                raise NotImplementedError('slice backward not supported')

            data = pandas.read_excel(self.parent._get_path(), parse_cols=[self.column], **self.parent.args)
            for row in data.values[start:stop]:
                yield row[0]

        def __getitem__(self, key):
            if isinstance(key, (int, long)):
                return self.__get_key(key)
            elif isinstance(key, slice):
                return self.__get_slice(key)
            raise ValueError('key is not an int, long or slice')    

    def __init__(self, path, args, columns_added=[], columns_hidden=[]):
        super(Excel, self).__init__(columns_added=columns_added, columns_hidden=columns_hidden)
        # TODO: Validate path, args, ...
        self.path = path
        self.args = args

    def _get_path(self):
        if base_path:
            return os.path.join(base_path, self.path)
        return self.path

    @staticmethod
    def from_json(payload):
        return Excel(
            payload['path'],
            args=payload['args'],
            columns_added=Table._decode_columns_added(payload),
            columns_hidden=Table._decode_columns_hidden(payload))

    def to_json(self):
        return dict(
            name='excel',
            payload=dict(
                path=self.path,
                args=self.args,
                columns_added=self._encode_columns_added(),
                columns_hidden=self._encode_columns_hidden()))

    def _get_iterator(self):
        data = pandas.read_excel(self._get_path(), **self.args)
        schema = get_chunk_schema(data)
        for row in data.values:
            yield self._new_tuple(schema, row)    

    def _get_keys(self):
        try:
            chunk = next(iter(pandas.read_csv(self._get_path(), chunksize=self.CHUNK_SIZE, **self.args)))
        except StopIteration:
            return []
        else:
            return [name for name in chunk.columns]

    def _get_key(self, key):
        if key < 0:
            raise NotImplementedError('index backward not support')

        data = pandas.read_excel(self._get_path(), **self.args)
        schema = get_chunk_schema(data)
        return self._new_tuple(schema, data.values[key])

    def _get_slice(self, slice):
        if slice.step:
            raise NotImplementedError('slice step not supported')

        start, stop = slice.start, slice.stop

        if not start:
            start = 0

        if stop is not None and stop < start:
            raise NotImplementedError('slice backward not supported')
        
        data = pandas.read_excel(self._get_path(), **self.args)
        schema = get_chunk_schema(data)
        for row in data.values[start:stop]:
            yield self.new_tuple(schema, row)

    def _get_column(self, name):
        raise NotImplementedError('Panda read_excel.parse_cols not working')
        #return Excel.Column(self, name)
