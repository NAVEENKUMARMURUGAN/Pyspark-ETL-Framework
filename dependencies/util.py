"""
udf.py
~~~~~~~~

Module containing Helper Utilities
"""
import json

from dependencies.exception import UdfUnavailable
from dependencies.udf import *


def hive_ddl_from_df(df, database, table_name, path, partition_columns):
    """
    source schema from Dataframe and generates Hive DDL
    :param path: Location of the input file for the External table
    :param table_name: Hive table name
    :param database: Hive Database name
    :param df: Dataframe
    :param partition_columns: partition columns
    """
    schema = json.loads(df.schema.json())
    partition_columns_str = ",\n".join([k + " " + v for k, v in partition_columns.items()])
    pc = f"PARTITIONED BY ({partition_columns_str})" if partition_columns else ""
    columns = ",\n".join([column['name'].lower() + " " + column['type'].lower()
                          for column in schema['fields']
                          if column['name'] not in partition_columns.keys()])
    hive_ddl = f""" CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table_name} (
                    {columns} 
                    )
                    {pc}
                    STORED AS PARQUET
                    LOCATION '{path}'

                 """.strip()
    return hive_ddl


def register_udf(spark, udfs):
    """
    Register Custom UDF's
    :param spark: Spark Session
    :param udfs: UDFs to be registered
    :return:
    """
    available_udfs = ["tominutes"]
    unavailable_udf = [udf for udf in udfs if udf not in available_udfs]
    if len(unavailable_udf) != 0:
        e = "UDF not available: " + ",".join(unavailable_udf)
        raise UdfUnavailable(e)

    for udf in udfs:
        if udf == 'tominutes':
            spark.udf.register("tominutes", tominutes)
            return None


