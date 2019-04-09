                             recipes-etl 
                             
******************************************************************
                               ETL
******************************************************************
                      Packaging ETL Job Dependencies
In this project, functions that can be used across different ETL jobs are kept in a module called dependencies and referenced in specific job modules using, for example,

from dependencies.spark import start_spark
This package, together with any additional dependencies referenced within it, must be to copied to each Spark node for all jobs that use dependencies to run. This can be achieved in one of several ways:

send all dependencies as a zip archive together with the job, using --py-files with Spark submit;
formally package and upload dependencies to somewhere like the PyPi archive (or a private version) and then run pip3 install dependencies on each node; or,
a combination of manually copying new modules (e.g. dependencies) to the Python path of each node and using pip3 install for additional dependencies (e.g. for requests).
Option (1) is by far the easiest and most flexible approach, so we will make use of this for now. To make this task easier, especially when modules such as dependencies have additional dependencies (e.g. the requests package), we have provided the build_dependencies.sh bash script for automating the production of packages.zip, given a list of dependencies documented in Pipfile and managed by the pipenv python application (discussed below).

                          Running the ETL job
Assuming that the $SPARK_HOME environment variable points to your local Spark installation folder, then the ETL job can be run from the project's root directory using the following command from the terminal,

`$SPARK_HOME/bin/spark-submit \
--master local[*] \
--py-files dependencies.zip \
--files configs/etl_config.json configs/transformation.sql \
jobs/etl_job.py
Briefly, the options supplied serve the following purposes:`

--master local[*] - the address of the Spark cluster to start the job on. If you have a Spark cluster in operation (either in single-executor mode locally, or something larger in the cloud) and want to send the job there, then modify this with the appropriate Spark IP - e.g. spark://the-clusters-ip-address:7077;
--files configs/etl_config.json - the (optional) path to any config file that may be required by the ETL job;
--py-files packages.zip - archive containing Python dependencies (modules) referenced by the job; and,
jobs/etl_job.py - the Python module file containing the ETL job to execute.

                  Passing Configuration Parameters to the ETL Job
Although it is possible to pass arguments to etl_job.py, as you would for any generic Python module running as a 'main' program - by specifying them after the module's filename and then parsing these command line arguments - this can get very complicated, very quickly, especially when there are lot of parameters (e.g. credentials for multiple databases, table names, SQL snippets, etc.). This also makes debugging the code from within a Python interpreter is extremely awkward, as you don't have access to the command line arguments that would ordinarily be passed to the code, when calling it from the command line.

A much more effective solution is to send Spark a separate file - e.g. using the --files configs/etl_config.json flag with spark-subit - containing the configuration in JSON format, which can be parsed into a Python dictionary in one line of code with json.loads(config_file_contents). Testing the code from within a Python interactive console session is also greatly simplified, as all one has to do to access configuration parameters for testing, is to copy and paste the contents of the file - e.g.,

import json

config = json.loads("""{
  "extract"  : {"uri": "tests/test_data/recipes/recipes.json",
                "clean": "True",
                "temptable": "recipes"},
  "transform": {"sql_path": "configs/transformation.sql",
                "udfs_required":["tominutes"]},
  "load"     : {"database": "hellofresh",
                "tablename": "recipes",
                "load_path": "user/hive/warehouse/hellofresh.db/recipes",
                "partition_cols": {"difficulty": "string"}
                },
  "impala"     : {"impala_host": "localhost",
                  "database": "hellofresh",
                  "tablename": "recipes"
                 }
}""")
For the exact details of how the configuration file is located, opened and parsed, please see the start_spark() function in dependencies/spark.py (also discussed further below), which in addition to parsing the configuration file sent to Spark (and returning it as a Python dictionary), also launches the Spark driver program (the application) on the cluster and retrieves the Spark logger at the same time.
Details about etl_config.json:: 

extract: 
       uri: path to the input file
       clean: remove extra white space and non-printable 
       temptable: Temptable name to register extract as table
       
transform: 
        sql_path: path to transformation sql
        udfs_required: udf's required for transformation
        
load:
        databasename: database name to create if not exist
        tablename: tablename to create if not exist 
        load_path: Hive external table path
        partition_cols: partition column names
        
impala: (refreshing impala for immediate querying)
        impala_host: host/port 
        databasename: databasename to refresh impala
        tablename: table name to refresh imapala
        
 

                             transformation.sql:

SELECT *,
       CASE
           WHEN total_time > 60 THEN 'hard'
           WHEN total_time BETWEEN 31 AND 60 THEN 'medium'
           WHEN total_time <= 30 THEN 'easy'
           ELSE 'unknown'
       END AS difficulty,
       cast(CURRENT_DATE AS string)  AS date_of_execution
FROM
  (SELECT *,
          tominutes(preptime) + tominutes(cooktime) AS total_time
   FROM recipes) b
WHERE Lower(ingredients) LIKE '%beef%'

*************************************************************************************************
                                       Pycharm Test Run
**************************************************************************************************

1) clone this project and Add spark jars and Py4j jars to content root
2) run jobs/etl_job.py
                                       Note
input file path: recipes-etl\tests\test_data\recipes\recipes.json
                                 ****important***
              I keep output file here for your review just in case any environmental issue!
output files path: recipes-etl\user\hive\warehouse\hellofresh.db\recipes

                                       Hi Reviewer!
    Thanks for reviewing the code! I have configured ETL pipeline via Json instead of passing arguments from CLI 
    reason being Good Practice! By this way we can generalize this framework not just for processing json, we can
    tweak to process various data source!