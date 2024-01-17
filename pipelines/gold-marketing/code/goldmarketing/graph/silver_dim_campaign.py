from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from goldmarketing.config.ConfigStore import *
from goldmarketing.udfs.UDFs import *

def silver_dim_campaign(spark: SparkSession) -> DataFrame:
    return spark.read.table("`hive_metastore`.`rainforest_silver`.`dim_campaign`")
