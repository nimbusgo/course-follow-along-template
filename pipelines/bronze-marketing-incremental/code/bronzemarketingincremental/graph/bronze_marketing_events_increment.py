from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from bronzemarketingincremental.config.ConfigStore import *
from bronzemarketingincremental.udfs.UDFs import *

def bronze_marketing_events_increment(spark: SparkSession, in0: DataFrame):
    from pyspark.sql.utils import AnalysisException

    try:
        desc_table = spark.sql("describe formatted `hive_metastore`.`rainforest_bronze`.`marketing_events`")
        table_exists = True
    except AnalysisException as e:
        table_exists = False

    in0.write\
        .format("delta")\
        .option("replaceWhere", f"dt = '{Config.dt}'")\
        .mode("overwrite")\
        .partitionBy("dt")\
        .saveAsTable("`hive_metastore`.`rainforest_bronze`.`marketing_events`")
