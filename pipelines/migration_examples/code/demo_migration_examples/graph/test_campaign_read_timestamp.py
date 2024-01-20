from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from demo_migration_examples.config.ConfigStore import *
from demo_migration_examples.udfs.UDFs import *

def test_campaign_read_timestamp(spark: SparkSession) -> DataFrame:
    return spark.sql(
        'SELECT * FROM `hive_metastore`.`rainforest_silver`.`test_campaign_base` TIMESTAMP AS OF "2023-12-19 20:14:25"'
    )
