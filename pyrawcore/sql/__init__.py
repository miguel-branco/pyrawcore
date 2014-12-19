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
from .sql import SQL


def sql(query, **tables):
    """Creates a SQL query resource.

    :param query: SQL query.
    :type query: str or unicode
    :param args: Arguments mapping table name to Resource instances. See usage example below.

    Usage example:

    Given the CSV file `data.csv` with contents::

        a,b
        1,2
        3,4

    To execute a SQL query over the file:

    >>> from raw.resources.csv import csv
    >>> data = csv('data.csv')
    >>> from raw.resources.sql import sql
    >>> resource = sql('SELECT * FROM table1 WHERE a > 1', table1=data)
    >>> list(resource)
    [(3L, 4L)]

    """
    return SQL(query, tables)


def load(payload):
    return SQL.from_json(payload)

__all__ = ['sql', 'load']
