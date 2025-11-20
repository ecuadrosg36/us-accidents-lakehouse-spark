"""
Configuration loader for Spark Lakehouse.
"""

import os
from pathlib import Path
from typing import Optional
import yaml
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class Config:
    """Centralized configuration for Spark Lakehouse."""
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to config YAML file (default: config/config.yaml)
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / "config" / "config.yaml"
        
        with open(config_path, 'r') as f:
            self._config = yaml.safe_load(f)
    
    # Paths
    @property
    def raw_data_path(self) -> str:
        return os.getenv("RAW_DATA_PATH", self._config.get("paths", {}).get("raw_data", "data/raw"))
    
    @property
    def bronze_path(self) -> str:
        return os.getenv("BRONZE_PATH", self._config.get("paths", {}).get("bronze", "data/bronze"))
    
    @property
    def silver_path(self) -> str:
        return os.getenv("SILVER_PATH", self._config.get("paths", {}).get("silver", "data/silver"))
    
    @property
    def gold_path(self) -> str:
        return os.getenv("GOLD_PATH", self._config.get("paths", {}).get("gold", "data/gold"))
    
    @property
    def checkpoint_path(self) -> str:
        return self._config.get("paths", {}).get("checkpoint", "checkpoints")
    
    @property
    def model_path(self) -> str:
        return self._config.get("paths", {}).get("models", "models")
    
    # Spark Settings
    @property
    def spark_app_name(self) -> str:
        return self._config.get("spark", {}).get("app_name", "US-Accidents-Lakehouse")
    
    @property
    def shuffle_partitions(self) -> int:
        return self._config.get("spark", {}).get("shuffle_partitions", 200)
    
    @property
    def enable_aqe(self) -> bool:
        return self._config.get("spark", {}).get("enable_aqe", True)
    
    # Data Settings
    @property
    def target_file_size_mb(self) -> int:
        return self._config.get("data", {}).get("target_file_size_mb", 128)


# Singleton instance
_config_instance: Optional[Config] = None


def get_config() -> Config:
    """Get singleton config instance."""
    global _config_instance
    if _config_instance is None:
        _config_instance = Config()
    return _config_instance
