from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from silver_fct_tables.config.ConfigStore import *
from silver_fct_tables.udfs.UDFs import *
from prophecy.utils import *
from silver_fct_tables.graph import *

def pipeline(spark: SparkSession) -> None:
    df_bronze_sales = bronze_sales(spark)
    df_FlattenProducts = FlattenProducts(spark, df_bronze_sales)
    df_bronze_products = bronze_products(spark)
    df_bronze_customers = bronze_customers(spark)
    df_JoinDimensions = JoinDimensions(spark, df_FlattenProducts, df_bronze_customers, df_bronze_products)
    df_FormatSales = FormatSales(spark, df_JoinDimensions)
    silver_fct_sales(spark, df_FormatSales)
    df_Reformat_1 = Reformat_1(spark, df_bronze_products)
    df_FormatOrders = FormatOrders(spark, df_JoinDimensions)
    silver_fct_orders(spark, df_FormatOrders)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("spark.sql.shuffle.partitions", "64")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/silver_fct_tables")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/silver_fct_tables", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/silver_fct_tables")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
