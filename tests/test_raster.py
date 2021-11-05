"""Tests for the raster utilities module."""
import doctest
import unittest  # noqa: F401

from aatoolbox.utils import raster


def load_tests(loader, tests, ignore):
    """Allow discovery of raster module doctests."""
    tests.addTests(doctest.DocTestSuite(raster))
    return tests
