import unittest

from typing import Iterable, Dict, List, Tuple
from collections import defaultdict

from .maths import lg
from .misc import T

from pyspark.sql import DataFrame as sparkDataFrame
from pyspark.sql import Row as sparkRow
from pandas import DataFrame as pandasDataFrame

from .probmonad import flip, return_, uniform
from .data import SparkFactory


def entropy_of_counts(counts: Iterable[int]) -> float:
    total = 0
    for count in counts:
        total += count
    total = float(total)
    ret = 0.0
    for count in counts:
        p = float(count) / total
        ret -= p * lg(p)
    return ret


def conditional_entropy_of_counts(counts: List[Tuple[T, int]]) -> float:
    totals = defaultdict(int)
    total = 0

    for (v, count) in counts:
        totals[v] += count
        total += count
    total = float(total)
    ret = 0.0

    for (v, count) in counts:
        t = float(totals[v])
        p1 = t/total
        p2 = float(count) / t
        ret -= p1 * p2 * lg(p2)

    return ret


def relative_entropy(d1, d2):
    ret = 0
    for v in d1.keys():
        p1 = d1[v]
        p2 = d2[v]
        ret -= p1 * lg(p2 / p1)
        return ret


def conditional_mutual_information(df, x, y, c):
    pass


class Pandas(object):
    @classmethod
    def _conditional_entropy(cls, df: pandasDataFrame, x: str, y: str) -> float:
        counts = (
            df
            .groupby([x, y])
            .size()
            .reset_index()[[y, 0]]
            .values
        )
        return conditional_entropy_of_counts(counts)

    @classmethod
    def _entropy(cls, df: pandasDataFrame, x: str):
        counts = list(df[x].value_counts())
        x_ent = entropy_of_counts(counts)
        return x_ent

    @classmethod
    def _entropy_series(cls, ds):
        counts = list(ds.value_counts())
        x_ent = entropy_of_counts(counts)
        return x_ent

    @classmethod
    def entropy(cls, df: pandasDataFrame, x: str):
        df = df[[x]]
        return cls._entropy(df, x)

    @classmethod
    def entropy_series(cls, ds):
        return cls._entropy_series(ds)

    @classmethod
    def mutual_information(cls, df: pandasDataFrame, x: str, y: str, normalize=False):
        df = df[[x, y]]

        # print(df)

        x_ent = cls._entropy(df, x)
        y_ent = cls._entropy(df, y)
        ent = min(x_ent, y_ent)
        x_cond_y_ent = cls._conditional_entropy(df, x, y)

        # print("x_ent=", x_ent)
        # print("y_ent=", y_ent)
        # print("x_cond_y_ent=", x_cond_y_ent)

        if normalize:
            if ent != 0.0:
                return (x_ent - x_cond_y_ent) / ent
            else:
                return 0.0
        else:
            return x_ent - x_cond_y_ent


class Spark(object):
    @staticmethod
    def _conditional_entropy(df: sparkDataFrame, x: str, y: str) -> float:

        df_count = df.groupBy(x, y)
        df_count = df_count.agg({'*': 'count'})
        counts = list(map(lambda r: (r[y], r['count(1)']), df_count.collect()))
        return conditional_entropy_of_counts(counts)

    @staticmethod
    def _entropy(df: sparkDataFrame, x: str):
        df_count = df.groupBy(x).agg({'*': 'count'})
        counts = list(map(lambda r: r['count(1)'], df_count.collect()))
        # misc.printme("len(counts): " + str(len(counts)) + "\n")
        x_ent = entropy_of_counts(counts)
        return x_ent

    @staticmethod
    def mutual_information(df: sparkDataFrame, x: str, y: str, normalize=False):
        df = df.selectExpr(x, y)

        x_ent = Spark._entropy(df, x)
        y_ent = Spark._entropy(df, y)
        ent = min(x_ent, y_ent)
        x_cond_y_ent = Spark._conditional_entropy(df, x, y)

        if normalize:
            if ent != 0.0:
                return (x_ent - x_cond_y_ent) / ent
            else:
                return 0.0
        else:
            return x_ent - x_cond_y_ent


# class TestInformationSpark(unittest.TestCase):
#     def setUp(self):
#         self.datahost = SparkFactory()
#
#         self.uni4 = uniform([0, 1, 2, 3]) >> (lambda x: return_(x,))
#         self.uni4_df = self.datahost.create(self.uni4.values(), columns=['x'])
#
#         self.indep = uniform([0, 1]) >> (
#             lambda x: uniform([0, 1]) >> (
#                 lambda y: return_(x, y)
#             )
#         )
#         self.indep_df = self.datahost.create(self.indep.values(), columns=['x', 'y'])
#
#     def tearDown(self):
#         self.datahost.close()
#
#     def runTest(self):
#         self.test_mutual_information()
#
#     def test_mutual_information(self):
#         df = self.indep_df
#
#         self.assertTrue(
#             Spark.mutual_information(df, "x", "y")
#             == 0.0
#         )


class TestInformationPandas(unittest.TestCase):
    def setUp(self):
        self.uni4 = uniform([0, 1, 2, 3]) >> (lambda x: return_(x,))
        self.uni4_df = pandasDataFrame(list(self.uni4.values()), columns=['x'])

        self.indep = uniform([0, 1]) >> (
            lambda x: uniform([0, 1]) >> (
                lambda y: return_(x, y)
            )
        )
        self.indep_df = pandasDataFrame(list(self.indep.values()), columns=['x', 'y'])

        self.dep = uniform([0, 1, 2, 3]) >> (
            lambda x: uniform([0, 1, 2, 3]) >> (
                lambda y: return_(x, y, (x, (y % 2)))
            )
        )
        self.dep_df = pandasDataFrame(list(self.dep.values()), columns=['x', 'y', 'z'])

    def tearDown(self):
        pass

    def test_information_of_counts(self):
        self.assertEqual(
            0.0,
            entropy_of_counts([10])
        )

        self.assertEqual(
            1.0,
            entropy_of_counts([5, 5])
        )

        self.assertEqual(
            2.0,
            entropy_of_counts([1, 1, 1, 1])
        )

        self.assertEqual(
            0.0,
            conditional_entropy_of_counts(
                [(0, 2)]
            )
        )

        self.assertEqual(
            1.0,
            conditional_entropy_of_counts(
                [(0, 1), (0, 1)]
            )
        )

        self.assertEqual(
            0.0,
            conditional_entropy_of_counts(
                [(0, 1), (1, 1)]
            )
        )

    def test_mutual_information(self):
        df = self.indep_df
        df_dep = self.dep_df

        self.assertEqual(
            0.0,
            Pandas.mutual_information(df, "x", "y")
        )

        self.assertEqual(
            2.0,
            Pandas.mutual_information(df_dep, "x", "z")
        )

        self.assertEqual(
            1.0,
            Pandas.mutual_information(df_dep, "y", "z")
        )


if __name__ == '__main__':
    unittest.main()
