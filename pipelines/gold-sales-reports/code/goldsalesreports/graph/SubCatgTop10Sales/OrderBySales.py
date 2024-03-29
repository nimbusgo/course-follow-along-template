from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from goldsalesreports.udfs.UDFs import *

def OrderBySales(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.orderBy(col("total_sales_usd_cents").desc())
