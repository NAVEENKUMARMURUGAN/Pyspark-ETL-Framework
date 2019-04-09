"""
etlcomponents.py
~~~~~~~~

Module containing ETL components
"""
from pyspark import SparkFiles
from pyspark.sql.functions import regexp_replace

from os import listdir, path
import os

from dependencies.util import *


class Extract(object):
    """
    ETL Extract Component
    """
    def __init__(self, extract_config):
        self.uri = extract_config.get('uri')
        self.clean = extract_config.get('clean', True)
        self.temptable = extract_config.get('temptable')

    def execute(self, spark):
        """Load data from json file format.

        :param spark: Spark session object.
        :return: Spark DataFrame.
        """
        df = (
            spark
                .read
                .json(self.uri)
        )

        cleaned_df = self.remove_non_printable(df)
        cleaned_df.createOrReplaceTempView(self.temptable)

        return cleaned_df

    def remove_non_printable(self, df):
        for column in df.columns:
            cleaned_df = df.withColumn(column, regexp_replace(column, '[\x7f\x80]', ""))
        return cleaned_df



class Transform(object):
    """
    ETL Transform component
    """
    def __init__(self, transform_config):
        self.sql_path = transform_config.get('sql_path', 'sql/transformation.sql')
        self.udfs_required = transform_config.get('udfs_required', ["tominutes"])

    def execute(self, spark):
        """Transform original dataset.

        :param spark: Spark Session Object
        :return: Transformed DataFrame.
        """
        register_udf(spark, self.udfs_required)
        sql = self.get_sql()
        df_transformed = spark.sql(sql)

        return df_transformed

    def get_sql(self):
        """
        Reads SQL from from file
        :return: SQL query
        """
        spark_files_dir = SparkFiles.getRootDirectory()
        sql_files = [filename
                     for filename in listdir(spark_files_dir)
                     if filename.endswith('transformation.sql')]
        if len(sql_files) != 0:
            path_to_sql_file = path.join(spark_files_dir, sql_files[0])
            with open(path_to_sql_file, 'r') as sql_file:
                sql = sql_file.read()
            # spark_logger.warn('loading transformation sql from ' + sql_files[0])
        else:
            # spark_logger.warn('no sql file found in directory' + spark_files_dir)
            with open('configs/transformation.sql', 'r') as sql_file:
                sql = sql_file.read()
        return sql


class Load(object):
    """
    ETL Load Component
    """
    def __init__(self, load_config):
        self.load_path = load_config.get('load_path', '/user/hive/warehouse/hellofresh.db/recipes')
        self.database = load_config.get('database')
        self.tablename = load_config.get('tablename')
        self.partition_cols = load_config.get('partition_cols', {"difficulty": "string"})
        self.partition_column = ",".join(self.partition_cols)

    def execute(self, spark, df, environment):
        """Load Executor
        :param environment: local or cluster flag
        :param spark: Spark Session
        :param df: DataFrame to load into parquet hive table
        :return: None
        """
        if environment != 'local':
            self.create_database_table(spark, df)

        (df
         .write
         .format('parquet')
         .mode('append')
         .partitionBy(self.partition_column)
         .save(self.load_path)
         )

        return None

    def create_database_table(self, spark, df):
        """Creates Hive Database and Hive Tables in not Exist
        :param spark: Spark Session
        :param df: DataFrame to load into parquet hive table
        :return: None
        """
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        ddl = hive_ddl_from_df(df, self.database, self.tablename, self.load_path, self.partition_cols)
        print(ddl)
        spark.sql(ddl)
        return None


class Impala(object):
    """
    Impala object to refresh in-order to run impala query as soon as data Loaded to hive
    """
    def __init__(self, misc_config):
        self.impala_host = misc_config.get('impala_host', 'localhost')
        self.database = misc_config.get('database', 'default')
        self.tablename = misc_config.get('tablename', 'recipes')

    def impala_refresh(self):
        os.system(f"impala-shell -i {self.impala_host} -q 'REFRESH {self.database}.{self.tablename}'")
        os.system(f"impala-shell -i {self.impala_host} -q 'INVALIDATE METADATA {self.database}.{self.tablename}'")
