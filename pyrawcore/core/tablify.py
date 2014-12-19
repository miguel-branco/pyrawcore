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


name_types = (str, unicode)
primitive_types = (int, long, float, str, unicode, bool)

def is_namedtuple(x):
  t = type(x)
  b = t.__bases__
  if len(b) != 1 or b[0] != tuple:
      return False
  f = getattr(t, '_fields', None)
  if not isinstance(f, tuple):
      return False
  return all(type(n) == str for n in f)

def get_primitive_type(v):
    if isinstance(v, int): return int
    elif isinstance(v, long): return long
    elif isinstance(v, float): return float
    elif isinstance(v, str): return str
    elif isinstance(v, unicode): return unicode
    elif isinstance(v, bool): return bool
    raise TypeError

def is_table(data):
    try:
        try:
            item = next(iter(data))
        except StopIteration:
            return None
        else:
            if isinstance(item, collections.OrderedDict) \
                and all([isinstance(k, name_types) for k in item.keys()]):
                schema = collections.OrderedDict()
                for k, v in item.items():
                    schema[k] = get_primitive_type(v)
                return schema
            elif isinstance(item, dict) \
                and all([isinstance(k, name_types) for k in item.keys()]):
                schema = dict()
                for k, v in item.items():
                    schema[k] = get_primitive_type(v)
                return schema
            elif is_namedtuple(item) \
                and all([isinstance(f, name_types) for f in item._fields]) \
                and all([isinstance(getattr(item, f), primitive_types) for f in item._fields]):
                # List of collections.namedtuple with primitive types
                schema = collections.OrderedDict()
                for k in item._fields:
                    schema[k] = get_primitive_type(getattr(item, k))
                return schema
            return None
    except TypeError:
        return None
