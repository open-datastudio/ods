import unittest
import tempfile

import opends

class TestOpends(unittest.TestCase):
    def test_initialize(self):
        opends.init()
