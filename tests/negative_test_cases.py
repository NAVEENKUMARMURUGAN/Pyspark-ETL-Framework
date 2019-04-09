"""
negative_test_cases.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in etl_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""

import unittest

from dependencies.spark import start_spark
from dependencies.etlcomponents import *


class SparkETLTestsNeg(unittest.TestCase):
    """Test suite for transformation in etl_job.py

    *****important**************
    sample data details : { total records: 1,
                            no of records contains beef in ingredient: 1 ,
                            no of records doesn't contain beef in in ingredients: 0
                            }
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        self.spark, self.log, *_ = start_spark(
            app_name='my_etl_job')

        self.config = json.loads("""{
                      "extract"  : {"uri": "tests/test_data/udf_test_data/recipes_negative.json",
                                    "clean": "True",
                                    "temptable": "recipes"},
                      "transform": {"sql_path": "configs/transformation.sql",
                                    "udfs_required":["tominutes"]},
                      "load"     : {"database": "hellofresh",
                                    "tablename": "recipes",
                                    "load_path": "user/hive/warehouse/hellofresh.db/recipes",
                                    "partition_cols": {"difficulty": "string"}
                                    },
                      "impala"     : {"impala_host": "localhost"}
                      }

                      """)

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_udf_tominute_neg(self):
        """
        Expectation: What happens to Empty or incorrect Prep or Cook Time
        """

        df = Extract(self.config["extract"]).execute(self.spark)

        df = Transform(self.config["transform"]).execute(self.spark)

        df.select('difficulty', 'total_time').show()


if __name__ == '__main__':
    unittest.main()
