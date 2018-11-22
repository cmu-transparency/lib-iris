""" Misc utilities not yet sorted into individual packages. """

import os
# import sys
import pickle
import _pickle as cp
import math
import argparse
import json
import time
from pathlib import Path
from typing import TypeVar, Iterable, Mapping, Any

import vdom         # pylint: disable=import-error


# typing #

A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')

U = TypeVar('U')
V = TypeVar('V')
W = TypeVar('W')

X = TypeVar('X')
Y = TypeVar('Y')
Z = TypeVar('Z')

T = TypeVar('T')


# html/vdom #

html = vdom.create_component("html", allow_children=True)  # pylint: disable=invalid-name
head = vdom.create_component("head", allow_children=True)  # pylint: disable=invalid-name
body = vdom.create_component("body", allow_children=True)  # pylint: disable=invalid-name


# accessors #

def _0(atuple):
    return atuple[0]


def _1(atuple):
    return atuple[1]


def _2(atuple):
    return atuple[2]


def _3(atuple):
    return atuple[3]


def _4(atuple):
    return atuple[4]


def _5(atuple):
    return atuple[5]


# iterators

def frange(begin: float, end: float, width: float = 1.0) -> Iterable[float]:
    """ Range but for floats. """

    i = begin
    while i < end:
        yield i
        i += width


# dicts #

def less(amap: Mapping[A, B], exceptions: Iterable[A]) -> Mapping[A, B]:
    """Return a new dictionary with items in exceptions removed."""

    return {k: v for k, v in amap.items() if k not in exceptions}


# mixins #

class ConfigurationMixin(object):
    """An object to store a set of key-value pairs with restrictions on the set of keys allowed."""

    __slots__ = []

    def __init__(self, **kwargs):
        for key, val in kwargs.items():
            setattr(self, key, val)

    def set(self, key, val):
        """Map key to val, return self for chaining sets."""

        setattr(self, key, val)
        return self

    def get(self, key):
        """Get value at key."""

        return getattr(self, key)


class LessMixin(object):  # pylint: disable=too-few-public-methods
    """Provides the less method."""

    def less(self: Mapping[A, B], exceptions: Iterable[A]) -> Mapping[A, B]:
        """See less above."""

        return self.__class__(less(self, exceptions))


class DefaultReprMixin(object):  # pylint: disable=too-few-public-methods
    def __repr__(self):
        cname = type(self).__name__

        parts = {k: repr(v) for k, v in self.__dict__.items() if v is not self}
        parts_separated = {k: tab(v, skip_first=True)
                           if "\n" in v else v for k, v in parts.items()}

        parts_strings = []

        for k, v in parts_separated.items():
            if len(v) > 20:
                parts_strings.append(f"{k}={v}, \n")
            else:
                parts_strings.append(f"{k}={v}, ")

        parts_string = "".join(parts_strings)

        if "\n" in parts_string:
            parts_string = tab(parts_string) + "\n"

        return f"{cname}({parts_string})"


class MyJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'default'):
            return obj.default()
        # else:
            # print(f"object {obj} {obj.__class__} {obj.__class__.__bases__} is not JSONableMixin")
            # return json.JSONEncoder.default(self, str(obj))

        return json.JSONEncoder.default(self, obj)


class JSONableMixin(object):  # pylint: disable=too-few-public-methods
    # @classmethod
    # def fromJSON(cls, source: str):
    #     cls(**json.loads(source))

    def toJSON(self):
        return json.dumps(self, cls=MyJSONEncoder, indent=4)

    def default(self):
        parts = {
            k: v.default() if hasattr(v, 'default') else v
            for k, v in self.__dict__.items()
        }

        c = self.__class__
        m = c.__module__

        return {'__class__': f"{m}.{c.__name__}",
                **parts}


class PrintedvarsMixin(object):  # pylint: disable=too-few-public-methods
    """ Provides default str and repr functions. """

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return repr(self.__dict__)


class LocalsMixin(object):  # pylint: disable=too-few-public-methods
    """Provides the locals method."""

    def locals(self, skip=None):
        """Returns an objects attributes as a dictionary."""

        if skip is None:
            skip = []

        return {k: v for k, v
                in self.__dict__.items()
                if not (k.startswith('__')
                        and k.endswith('__'))
                and k not in skip}


# IO #

# print and flush

def printme(*ss):
    """Print and flush."""

    print(*ss, flush=True)


def which(*files):
    for file in files:
        if os.path.exists(file):
            return file

    return None


class Watch(object):
    def __init__(self, filename: str):
        self.filename = filename

        self.content = None
        self.updated = None
        self.update()

    def update(self):
        self.content = read_file(self.filename)
        self.updated = os.path.getmtime(self.filename)

    def get(self):
        if os.path.getmtime(self.filename) > self.updated:
            self.update()
        return self.content

    def __str__(self):
        return self.get()

    def __repr__(self):
        return f"Watch({self.filename})/updated={time.ctime(self.updated)}"


def watch_file(filename: str):
    return Watch(filename)


def read_file(filename: str):
    return Path(filename).read_text()


def load(filename: str, pickler=pickle):
    """Load an object from a file."""

    with open(filename, 'rb') as file:
        return pickler.load(file)


def load_or_new(filename: str, default: A, pickler=pickle) -> A:
    """Load a value from a pickle if it exists, otherwise use the default."""

    if os.path.exists(filename):
        return load(filename, pickler=pickler)

    return save(filename, default, pickler=pickler)


def save(filename: str, obj: A, pickler=pickle) -> A:
    """Save a value to a pickle."""

    with open(filename, 'wb') as file:
        pickler.dump(obj, file)
    return obj


def load_p2c(filename, pickler=pickle):
    with open(filename, 'rb') as file:
        return cp.load(file, encoding='latin1')


# maths #

def lg(d: float) -> float:  # pylint: disable=invalid-name
    """ Log base 2. """

    return math.log(d, 2.0)


# strings #

def tab(string: str, skip_first: bool = False) -> str:
    """Prepend each line in the given string with tab."""

    temp = [line if skip_first and line_num == 0 else u"\t" + line
            for line_num, line in enumerate(string.split(u"\n"))]

    return u"\n".join(temp)


# unsorted #

def named_of_indexed(items: Iterable[B], keys=None) -> Mapping[str, B]:
    """Given a list of items, create a dictionary that maps their names (their names themselves or
    their key in the optional keys argument) to the item. Used to convert lists that index by
    integer to dictionaries that index by name.
    """

    ret = {}
    for idx, item in enumerate(items):
        item = items[idx]
        if keys is not None:
            ret[str(keys[idx])] = item
        else:
            ret[str(item)] = item
    return ret


# collections #

class IdentityDictionary(dict):
    """A dictionary that always returns the key when asked for item at key."""

    def __getitem__(self, key):
        return key


class ArrayDict(dict):
    """Combination of array and dictionary, can be accessed either way."""

    def __init__(self, items_by_index, keys=None, **kwargs):
        """If the keys for the items are not provided, the items themselves become the keys."""

        if keys is None:
            keys = items_by_index

        dict.__init__(self, named_of_indexed(items_by_index, keys=keys), **kwargs)

        self.keys_by_index = keys
        self.items_by_index = items_by_index

    def __setitem__(self, index, v):
        if isinstance(index, int):
            self.items_by_index[index] = v
        else:
            dict.__setitem__(self, str(index), v)

    def __getitem__(self, index):
        if isinstance(index, int):
            return self.items_by_index[index]
        return dict.__getitem__(self, str(index))


# command line #

def get_args_string() -> str:
    """Command line parser for a single string argument."""

    parser = argparse.ArgumentParser()

    parser.add_argument("string", type=str)

    args = parser.parse_args()

    return args.string


def get_args_2strings() -> (str, str):
    """Command line parser for two string arguments."""

    parser = argparse.ArgumentParser()

    parser.add_argument("string1", type=str)
    parser.add_argument("string2", type=str)

    args = parser.parse_args()

    return (args.string1, args.string2)
