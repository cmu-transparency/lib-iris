from abc import ABC, abstractmethod
from typing import Iterable, Tuple

from . import misc
from .types import TBaseModel, TRow, TValue, TFeature


class Model(ABC):
    """A model."""

    def __init__(self,
                 model: TBaseModel = None,
                 filename: str = None) -> None:

        self._model = model
        if filename is not None:
            self.load(filename)

    def get_model(self) -> TBaseModel:
        return self._model

    def set_model(self, model: TBaseModel):
        self._model = model
        return self

    def load(self, filename: str):
        """Load model from file."""
        self._model = misc.load(filename)
        return self

    def save(self, filename: str):
        """Save the model to a file."""
        misc.save(filename, self._model)
        return self

    @abstractmethod
    def used_features(self) -> Iterable[TFeature]:
        """Get the set of features used in the classifier."""
        pass

    # @abstractmethod
    def predict(self, rows: Iterable[TRow]) -> Iterable[TValue]:
        return self._model.predict(rows)

    # @abstractmethod
    def predict_proba(self, rows: Iterable[TRow]) -> Iterable[Tuple[TValue, float]]:
        return self._model.predict_proba(rows)
