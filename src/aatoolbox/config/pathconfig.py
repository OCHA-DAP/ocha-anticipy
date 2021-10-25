"""Path configuration."""

import os
from dataclasses import dataclass
from pathlib import Path

BASE_DIR_ENV = "AA_DATA_DIR"


@dataclass
class PathConfig(object):
    """Global directory parameters."""

    public: str = "public"
    private: str = "private"
    raw: str = "raw"
    processed: str = "processed"

    def __init__(self, base_dir_env: str = BASE_DIR_ENV):
        self.base_dir_env = base_dir_env
        self.base_path = Path(os.environ[self.base_dir_env])
        super().__init__()
