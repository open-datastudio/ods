import unittest
import tempfile

import ods

class TestOds(unittest.TestCase):
    def test_initialize(self):
        ods.init()
