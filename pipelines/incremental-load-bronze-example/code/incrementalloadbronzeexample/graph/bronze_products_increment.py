from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from incrementalloadbronzeexample.config.ConfigStore import *
from incrementalloadbronzeexample.udfs.UDFs import *

def bronze_products_increment(spark: SparkSession, in0: DataFrame):
    from pyspark.sql.utils import AnalysisException

    try:
        desc_table = spark.sql("describe formatted `hive_metastore`.`rainforest_bronze`.`products`")
        table_exists = True
    except AnalysisException as e:
        table_exists = False

    in0.write\
        .format("delta")\
        .option("replaceWhere", f"date_introduced = '{Config.dt}'")\
        .mode("overwrite")\
        .partitionBy("date_introduced")\
        .saveAsTable("`hive_metastore`.`rainforest_bronze`.`products`")
