# encoding=utf8

"""
Utility functions for interacting with java from within python. See
also py4j.
"""


def class_name(obj):
    """Given a reference to a java object, return its simple class name."""

    # Note the below are java methods which get called using the py4j
    # system.

    return obj.getClass().getSimpleName()
