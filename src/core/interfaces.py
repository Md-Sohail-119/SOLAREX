"""Core interfaces for pipeline modules."""

from abc import ABC, abstractmethod

from .datatypes import FlareSequence


class PipelineStep(ABC):
    """Contract for all sequence-processing steps in the pipeline."""

    @abstractmethod
    def process(self, sequence: FlareSequence) -> FlareSequence:
        """Process and return a flare sequence payload."""
        raise NotImplementedError
