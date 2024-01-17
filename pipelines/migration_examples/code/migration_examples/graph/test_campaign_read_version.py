from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from migration_examples.config.ConfigStore import *
from migration_examples.udfs.UDFs import *

def test_campaign_read_version(spark: SparkSession) -> DataFrame:
    return spark.sql('SELECT * FROM `hive_metastore`.`rainforest_silver`.`test_campaign_base` VERSION AS OF 2')
