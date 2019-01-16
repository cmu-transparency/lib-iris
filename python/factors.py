import unittest

from abc import ABC, abstractmethod
from typing import Iterable
import numpy as np

from .probmonad import return_, uniform
from .types import TFeature, TValue, TRow, TDataset
from .model import Model
from . import metrics


class Factor(object):
    def __init__(self,
                 feature: TFeature,
                 actual_value: TValue = None,
                 predicted_value: TValue = None,
                 predicted_proba: float = None,
                 influence: float = None) -> None:

        self.feature = feature
        self.actual_value = actual_value
        self.predicted_value = predicted_value
        self.predicted_proba = predicted_proba
        self.influence = influence

    def __str__(self): return "Factor(%s=%s/%s @ %s/%0.3f)" % (
            str(self.feature),
            str(self.actual_value),
            str(self.predicted_value),
            str(self.predicted_proba),
            self.influence
    )

    def __repr__(self):
        return str(self)


class Factorizer(ABC):
    def __init__(self,
                 dataset: TDataset,
                 model: Model):

        self.dataset: TDataset = dataset
        self.model: Model = model

    @abstractmethod
    def factorize(self,
                  row: TRow,
                  features: Iterable[TFeature] = None) -> Iterable[Factor]:
        pass


class QiiFactorizer(Factorizer):
    def __init__(self, dataset, model, dist_in=None, dist_out=None) -> None:
        Factorizer.__init__(self, dataset, model)

        if dist_in is None:
            dist_in = lambda a, b: 1.0

        if dist_out is None:
            dist_out = lambda a, b: 1.0 if a != b else 0.0

        self.qii = metrics.QII(dataset, model, dist_in, dist_out)

    def factorize(self, row, features=None):
        ret = []

        if features is None:
            features = self.model.used_features()

        for factor in features:
            predicted, predicted_proba = self.model.predict_proba([row])[0]

            inf = self.qii.single(row, factor)

            ret.append(Factor(feature=factor,
                              actual_value=row[factor],
                              predicted_value=predicted,
                              predicted_proba=predicted_proba,
                              influence=inf
                              ))

        ret.sort(key=lambda i: (-abs(i.influence), -abs(i.actual_value)))

        return ret


class TestFactors(unittest.TestCase):
    def setUp(self):
        uni8 = uniform(np.arange(8, dtype=np.int64))

        self.indep2 = uni8.power(2)
        self.indep2_df = self.indep2.to_pandas(["x", "y"])

        self.indep3 = uni8.power(3)
        self.indep3_df = self.indep3.to_pandas(['x', 'y', 'z'])

        class TestModelDirect(Model):
            def __init__(self, i):
                self.i = i

            def used_features(self):
                return [0, 1]

            def predict(self, rows):
                return list(map(lambda row: row[self.i], rows))

            def predict_proba(self, rows):
                return list(map(lambda ret: (ret, 1.0), self.predict(rows)))

        self.model0 = TestModelDirect(0)
        self.model1 = TestModelDirect(1)

        self.qiifactorizer0 = QiiFactorizer(self.indep2_df, self.model0)
        self.qiifactorizer1 = QiiFactorizer(self.indep2_df, self.model1)

        class TestModelPartial(Model):
            def __init__(self):
                pass

            def used_features(self):
                return [0, 1, 2]

            def predict(self, rows):
                return list(map(lambda row:
                                (row[2] % 8) +
                                (row[0] % 4) +
                                (row[1] % 2),
                                rows))

            def predict_proba(self, rows):
                return list(map(lambda ret: (ret, 1.0), self.predict(rows)))

        self.qiifactorizer201 = QiiFactorizer(self.indep3_df, TestModelPartial())

    def tearDown(self):
        pass

    def test_qii_factorizer(self):
        temp = self.qiifactorizer0.factorize(np.array([0, 0], dtype=np.int64))
        assert(temp[0].feature == 0)
        assert(temp[1].influence == 0.0)

        temp = self.qiifactorizer1.factorize(np.array([0, 0], dtype=np.int64))
        assert(temp[0].feature == 1)
        assert(temp[1].influence == 0.0)

        temp = self.qiifactorizer201.factorize(np.array([0, 0, 0], dtype=np.int64))
        assert(list(map(lambda t: t.feature, temp)) == [2, 0, 1])
        assert(list(map(lambda t: t.influence, temp)) == [0.875, 0.75, 0.5])


if __name__ == '__main__':
    unittest.main()
