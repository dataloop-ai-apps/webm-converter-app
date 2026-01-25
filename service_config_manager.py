import logging
from typing import Dict, Any, Optional

import dtlpy as dl

logger = logging.getLogger(__name__)

GLOBAL_KEY = "*"


class ServiceConfigManager:
    """
    Manages service configurations loaded from dataset binaries datasets.

    Supports:
    - Per-dataset configuration overrides
    - Global (*) dataset-wide defaults
    - Hardcoded defaults as fallback
    - In-memory caching per dataset

    Config Resolution Priority: defaults < global (*) < dataset-specific
    """

    def __init__(self, service_name: str, defaults: Optional[Dict[str, Any]] = None):
        """
        Initialize the configuration manager.

        Args:
            service_name: The service identifier key in the config file
                         (e.g., "webm-converter", "video-thumbnail")
            defaults: Default configuration values to use when not specified in config file
        """
        self.service_name = service_name
        self.defaults = defaults or {}
        self._config_cache: Dict[str, Dict[str, Any]] = {}  # Keyed by dataset_id

    def get_config(self, dataset_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get the resolved configuration for a specific dataset and optionally dataset.

        Resolution priority: defaults < global (*) < dataset-specific

        Args:
            dataset_id: The Dataloop dataset ID
            dataset_id: Optional dataset ID for dataset-specific config overrides

        Returns:
            Resolved configuration dictionary with all applicable settings merged
        """
        return self._load_dataset_config(dataset_id=dataset_id)

    def _load_dataset_config(self, dataset_id: str) -> Dict[str, Any]:
        """
        Load and cache configuration from the dataset's binaries dataset.

        Args:
            dataset_id: The Dataloop dataset ID

        Returns:
            The service-specific configuration section from the config file,
            or empty dict if not found
        """
        
        # Return cached config if available
        if dataset_id in self._config_cache:
            logger.debug(f"Using cached config for dataset {dataset_id}")
            return self._config_cache[dataset_id]

        # Load from binaries dataset
        dataset = dl.datasets.get(dataset_id=dataset_id)
        project = dataset.project
        binaries_dataset = project.datasets._get_binaries_dataset()

        full_config = binaries_dataset.metadata.get("service_configs", {})
        # Extract service-specific config
        service_config = full_config.get(self.service_name, {})
        logger.info(f"Loaded config for service '{self.service_name}' from project {project.id}")
        dataset_config = self._resolve_config(service_config=service_config, dataset_id=dataset_id)

        # Cache the config
        self._config_cache[dataset_id] = dataset_config
        return dataset_config

    def _resolve_config(self, service_config: Dict[str, Any], dataset_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Resolve the final configuration by merging defaults, global, and dataset-specific settings.

        Priority: defaults < global (*) < dataset-specific

        Args:
            service_config: The service-specific configuration
            dataset_id: Optional dataset ID for dataset-specific overrides

        Returns:
            Merged configuration dictionary
        """
        # Start with defaults
        resolved = dict(self.defaults)

        # Apply global (*) config if exists
        global_config = service_config.get(GLOBAL_KEY, {})
        resolved.update(global_config)

        # Apply dataset-specific config if exists
        if dataset_id and dataset_id in service_config:
            dataset_specific_config = service_config.get(dataset_id, {})
            resolved.update(dataset_specific_config)
            logger.debug(f"Applied dataset-specific config {dataset_specific_config} for {dataset_id}")

        return resolved

    def clear_cache(self, dataset_id: Optional[str] = None) -> None:
        """
        Clear the configuration cache.

        Args:
            dataset_id: If provided, only clear cache for this dataset.
                       If None, clear all cached configs.
        """
        if dataset_id:
            if dataset_id in self._config_cache:
                del self._config_cache[dataset_id]
                logger.info(f"Cleared config cache for dataset {dataset_id}")
        else:
            self._config_cache.clear()
            logger.info("Cleared all config caches")

    def reload_config(self, dataset_id: str) -> Dict[str, Any]:
        """
        Force reload configuration from the binaries dataset.

        Args:
            dataset_id: The Dataloop dataset ID

        Returns:
            The freshly loaded service configuration
        """
        self.clear_cache(dataset_id=dataset_id)
        return self._load_dataset_config(dataset_id=dataset_id)
