from dataquality.graph.CustomersEarly.config.Config import SubgraphConfig as CustomersEarly_Config
from dataquality.graph.CustomersLater.config.Config import SubgraphConfig as CustomersLater_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, CustomersEarly: dict=None, CustomersLater: dict=None, **kwargs):
        self.spark = None
        self.update(CustomersEarly, CustomersLater)

    def update(self, CustomersEarly: dict={}, CustomersLater: dict={}, **kwargs):
        prophecy_spark = self.spark
        self.CustomersEarly = self.get_config_object(
            prophecy_spark, 
            CustomersEarly_Config(prophecy_spark = prophecy_spark), 
            CustomersEarly, 
            CustomersEarly_Config
        )
        self.CustomersLater = self.get_config_object(
            prophecy_spark, 
            CustomersLater_Config(prophecy_spark = prophecy_spark), 
            CustomersLater, 
            CustomersLater_Config
        )
        pass
