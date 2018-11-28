from abc import ABC, abstractmethod

from typing import Iterable, Tuple

from pyspark.sql import DataFrame as sparkDataFrame
from pyspark.sql import Row as sparkRow
from pandas import DataFrame as pandasDataFrame

from spark import Spark
from misc import T


class DataFrame(object):
    def __init__(self, df):
        self.df = df


class PandasDataFrame(DataFrame):
    def __init__(self, df: pandasDataFrame):
        DataFrame.__init__(self, df)


class SparkDataFrame(DataFrame):
    def __init__(self, df: sparkDataFrame):
        DataFrame.__init__(self, df)

    def selectExpr(self, *kargs, **kwargs):
        return SparkDataFrame(self.df.selectExpr(*kargs, **kwargs))

    def groupBy(self, *kargs, **kwargs):
        return SparkDataFrame(self.df.groupBy(*kargs, **kwargs))

    def agg(self, *kargs, **kwargs):
        return SparkDataFrame(self.df.agg(*kargs, **kwargs))

    def collect(self, *kargs, **kwargs):
        return SparkDataFrame(self.df.collect(*kargs, **kwargs))

    def __getitem__(self, k):
        return self.df[k]


class DataFactory(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def create(self, items: Iterable[T], columns: Iterable[str]) -> DataFrame:
        pass

    def close(self):
        pass


class PandasFactory(DataFactory):
    def __init__(self):
        DataFactory.__init__(self)

    def create(self, items, columns) -> PandasDataFrame:
        return PandasDataFrame(
            df=pandasDataFrame(data=list(items),
                               columns=columns))


class SparkFactory(DataFactory):
    spark = None

    def __init__(self, spark=None):
        DataFactory.__init__(self)

        if spark is None and SparkFactory.spark is not None:
            spark = SparkFactory.spark

        if spark is None:
            print("*** starting spark ***")

            self.spark = Spark()
            self.spark.start()

            SparkFactory.spark = self.spark

    def close(self):
        print("*** closing spark ***")

        self.spark.stop()

    def create(self, items: Iterable[Tuple[T]], columns: Iterable[str] = None) -> SparkDataFrame:
        rowClass = sparkRow(*columns)
        rdd = self.spark.parallelize(map(lambda i: rowClass(*i), items))

        print(rdd.collect())

        df = rdd.toDF()

        return SparkDataFrame(df=df)
        # return SparkDataFrame(df=self.spark.createDataFrame(list(items), columns))
