"""Data transfer objects for the flare data pipeline."""

from dataclasses import dataclass, field
from typing import List, Optional

import numpy as np


@dataclass
class FlareSequence:
    """Pipeline payload containing flare metadata and sequence artifacts."""

    event_id: str
    peak_time: str
    flare_class: str
    start_window: str
    end_window: str
    raw_fits_paths: List[str] = field(default_factory=list)
    processed_sequence: Optional[np.ndarray] = None
    is_valid: bool = True
