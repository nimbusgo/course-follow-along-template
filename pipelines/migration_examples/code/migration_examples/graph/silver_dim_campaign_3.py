from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from migration_examples.config.ConfigStore import *
from migration_examples.udfs.UDFs import *

def silver_dim_campaign_3(spark: SparkSession) -> DataFrame:
    return spark.read.table("`hive_metastore`.`rainforest_silver`.`dim_campaign`")
