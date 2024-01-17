from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from dataquality.config.ConfigStore import *
from dataquality.udfs.UDFs import *

def CountMissing(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(sum(expr("if(isnull(customer_customer_id), 1, 0)")).alias("missing_customer_id_count"))