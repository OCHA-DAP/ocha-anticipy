"""Path configuration."""

import os
from dataclasses import dataclass
from pathlib import Path

_BASE_DIR_ENV = "AA_DATA_DIR"


@dataclass
class PathConfig:
    """Global directory parameters."""

    base_dir_env: str = _BASE_DIR_ENV
    public: str = "public"
    private: str = "private"
    raw: str = "raw"
    processed: str = "processed"

    def __post_init__(self):
        """Set the base path based on the environemnt variable."""
        self.base_path = Path(os.environ[self.base_dir_env])
