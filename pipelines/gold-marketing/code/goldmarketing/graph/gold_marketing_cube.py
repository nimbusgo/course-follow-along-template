from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from goldmarketing.config.ConfigStore import *
from goldmarketing.udfs.UDFs import *

def gold_marketing_cube(spark: SparkSession, in0: DataFrame):
    from pyspark.sql.utils import AnalysisException

    try:
        desc_table = spark.sql("describe formatted `hive_metastore`.`rainforest_gold`.`marketing_rollup`")
        table_exists = True
    except AnalysisException as e:
        table_exists = False

    in0.write.format("delta").mode("overwrite").saveAsTable("`hive_metastore`.`rainforest_gold`.`marketing_rollup`")
