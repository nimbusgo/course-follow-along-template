from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from migration_examples.config.ConfigStore import *
from migration_examples.udfs.UDFs import *

def AddTS2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn("as_of", current_timestamp())
