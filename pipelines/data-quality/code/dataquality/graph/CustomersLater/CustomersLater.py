from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *

def CustomersLater(spark: SparkSession, subgraph_config: SubgraphConfig) -> DataFrame:
    Config.update(subgraph_config)
    df_raw_customer_snapshot_1_1 = raw_customer_snapshot_1_1(spark)
    subgraph_config.update(Config)

    return df_raw_customer_snapshot_1_1
