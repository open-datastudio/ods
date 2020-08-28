import unittest
import tempfile

import opends
import os

def integration_test_ready():
    return "STAROID_ACCESS_TOKEN" in os.environ

class TestSpark(unittest.TestCase):
    def test_initialize(self):
        opends.init()
        spark = opends.spark("test")

    @unittest.skipUnless(integration_test_ready(), "Integration test environment is not configured")
    def test_run_spark_job(self):
        opends.init()
        spark = opends.spark("test")
        df = spark.createDataFrame([{"hello": "world"} for x in range(100)])
        self.assertEqual(100, df.count())

