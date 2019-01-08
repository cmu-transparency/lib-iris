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

    def __init__(self, model: TBaseModel = None,
                 filename: str = None) -> None:
        self._model = model
        if filename is not None:
            self.load(filename)

    def get_model(self):
        return self._model

    def set_model(self, model):
        self._model = model

    def load(self, filename):
        """Load model from file."""
        self._model = misc.load(filename)
        return self

    def save(self, filename):
        """Save the model to a file."""
        misc.save(filename, self._model)
        return self

    @abstractmethod
    def used_features(self):
        """Get the set of features used in the classifier."""
        pass

    @abstractmethod
    def predict(self, row):
        pass
