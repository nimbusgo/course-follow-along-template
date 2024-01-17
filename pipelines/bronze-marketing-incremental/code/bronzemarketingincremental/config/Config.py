from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, dt: str=None, **kwargs):
        self.spark = None
        self.update(dt)

    def update(self, dt: str="2023-11-01", **kwargs):
        prophecy_spark = self.spark
        self.dt = dt
        pass
