from abc import ABC, abstractmethod
from typing import Iterable

from model import TFeature, TValue, TRow, TDataset, Model
import metrics


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
    def factorize(self, row: TRow, features: Iterable[TFeature] = None) -> Iterable[Factor]:
        pass


class QiiFactorizer(Factorizer):
    def __init__(self, dataset, model, dist_in, dist_out) -> None:
        Factorizer.__init__(self, dataset, model)

        self.qii = metrics.QII(dataset, model, dist_in, dist_out)

    def factorize(self, row, features=None):
        ret = []

        if features is None:
            features = self.model.used_features()

        for factor in features:
            predicted = self.model.predict(row)

            inf = self.qii(row, factor)

            ret.append(Factor(feature=factor,
                              actual_value=row[factor],
                              predicted_value=predicted.mode,
                              predicted_proba=predicted.confidence,
                              influence=inf
                ))

        ret.sort(key=lambda i: (-abs(i.influence), -abs(i.actual_value)))

        return ret
