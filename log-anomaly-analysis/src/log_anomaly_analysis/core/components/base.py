"""
Base component interface
"""

from abc import ABC, abstractmethod
from typing import Any, Dict

import polars as pl


class BaseComponent(ABC):
    """Base class for all pipeline components"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._validate_config()

    @abstractmethod
    def _validate_config(self):
        """Validate component-specific configuration"""
        pass

    @abstractmethod
    def process(self, data: pl.DataFrame) -> pl.DataFrame:
        """Process the input data"""
        pass
