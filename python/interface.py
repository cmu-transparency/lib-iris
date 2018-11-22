from abc import ABC, abstractmethod
from . import misc

from typing import TypeVar, Iterable

TFeature = TypeVar('feature')
TRow = TypeVar('dataset row')
TDataset = TypeVar('dataset')
TBaseModel = TypeVar('base model')
TValue = TypeVar('value')


class Model(ABC):
    """A model."""

    def __init__(self, basemodel: TBaseModel = None) -> None:
        self.basemodel = basemodel

    def load(self, filename):
        """Load model from file."""
        self.basemodel = misc.load(filename)
        return self

    def save(self, filename):
        """Save the model to a file."""
        misc.save(filename, self.basemodel)
        return self

    @abstractmethod
    def used_features(self):
        """Get the set of features used in the classifier."""
        pass


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
