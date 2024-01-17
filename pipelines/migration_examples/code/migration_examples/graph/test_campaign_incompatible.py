from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from migration_examples.config.ConfigStore import *
from migration_examples.udfs.UDFs import *

def test_campaign_incompatible(spark: SparkSession, in0: DataFrame):
    from pyspark.sql.utils import AnalysisException

    try:
        desc_table = spark.sql("describe formatted `hive_metastore`.`rainforest_silver`.`test_campaign_base`")
        table_exists = True
    except AnalysisException as e:
        table_exists = False

    in0.write\
        .format("delta")\
        .option("mergeSchema", True)\
        .mode("append")\
        .saveAsTable("`hive_metastore`.`rainforest_silver`.`test_campaign_base`")
