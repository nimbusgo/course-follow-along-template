from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from incrementalloadbronzeexample.config.ConfigStore import *
from incrementalloadbronzeexample.udfs.UDFs import *

def bronze_sales_increment(spark: SparkSession, in0: DataFrame):
    from pyspark.sql.utils import AnalysisException

    try:
        desc_table = spark.sql("describe formatted `rainforest_bronze`.`sales`")
        table_exists = True
    except AnalysisException as e:
        table_exists = False

    in0.write\
        .format("delta")\
        .option("mergeSchema", True)\
        .option("replaceWhere", f"sale_date = '{Config.dt}'")\
        .mode("overwrite")\
        .partitionBy("sale_date")\
        .saveAsTable("`rainforest_bronze`.`sales`")
