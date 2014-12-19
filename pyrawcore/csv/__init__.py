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
from .csv import Csv


def csv(path, **args):
    """Creates a query-able RAW resource from a CSV file.

    :param path: The CSV file path.
    :type path: str or unicode
    :param args: Arguments to pass to the internal (Pandas-based) file parser. Accepts all arguments in `pandas.read_csv <http://pandas.pydata.org/pandas-docs/stable/generated/pandas.io.parsers.read_csv.html>`_.

    Usage example:

    >>> resource = csv('/home/john/data.xlsx')

    """
    return Csv(path, args=args)


def load(payload):
    return Csv.from_json(payload)

__all__ = ['csv', 'load']
