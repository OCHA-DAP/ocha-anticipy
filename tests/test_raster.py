"""Tests for the raster utilities module."""
import doctest
import unittest  # noqa: F401

from aatoolbox.utils import raster

suite = unittest.TestSuite()
suite.addTest(doctest.DocTestSuite(raster))
unittest.TextTestRunner().run(suite)
