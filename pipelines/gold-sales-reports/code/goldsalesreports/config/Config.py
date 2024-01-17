from goldsalesreports.graph.SubCatgTop10Sales.config.Config import SubgraphConfig as SubCatgTop10Sales_Config
from goldsalesreports.graph.Top10Sales_1.config.Config import SubgraphConfig as Top10Sales_1_Config
from goldsalesreports.graph.MainCatTop10Sales.config.Config import SubgraphConfig as MainCatTop10Sales_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, SubCatgTop10Sales: dict=None, MainCatTop10Sales: dict=None, Top10Sales_1: dict=None, **kwargs):
        self.spark = None
        self.update(SubCatgTop10Sales, MainCatTop10Sales, Top10Sales_1)

    def update(self, SubCatgTop10Sales: dict={}, MainCatTop10Sales: dict={}, Top10Sales_1: dict={}, **kwargs):
        prophecy_spark = self.spark
        self.SubCatgTop10Sales = self.get_config_object(
            prophecy_spark, 
            SubCatgTop10Sales_Config(prophecy_spark = prophecy_spark), 
            SubCatgTop10Sales, 
            SubCatgTop10Sales_Config
        )
        self.MainCatTop10Sales = self.get_config_object(
            prophecy_spark, 
            MainCatTop10Sales_Config(prophecy_spark = prophecy_spark), 
            MainCatTop10Sales, 
            MainCatTop10Sales_Config
        )
        self.Top10Sales_1 = self.get_config_object(
            prophecy_spark, 
            Top10Sales_1_Config(prophecy_spark = prophecy_spark), 
            Top10Sales_1, 
            Top10Sales_1_Config
        )
        pass
