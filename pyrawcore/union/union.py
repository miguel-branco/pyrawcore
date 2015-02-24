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
from ..core import get_option, load


class Union(Table):

    def __init__(self, tables, columns_added=[], columns_hidden=[]):
        super(Union, self).__init__(columns_added=columns_added, columns_hidden=columns_hidden)
        self.tables = tables

    @staticmethod
    def from_json(payload):
        return Union(
            tables=[load(table) for table in payload['tables']],
            columns_added=Table._decode_columns_added(payload),
            columns_hidden=Table._decode_columns_hidden(payload))

    def to_json(self):
        return dict(
            name='union',
            payload=dict(
                tables=[table.to_json() for table in self.tables],
                columns_added=self._encode_columns_added(),
                columns_hidden=self._encode_columns_hidden()))

    def _get_iterator(self):
        for table in self.tables:
            for row in table:
                yield row

    def _get_keys(self):
        raise NotImplementedError('_get_keys')

    def _get_key(self, key):
        raise NotImplementedError('_get_key')

    def _get_slice(self, slice):
        raise NotImplementedError('_get_slice')

    def _get_column(self, name):
        raise NotImplementedError('_get_column')
