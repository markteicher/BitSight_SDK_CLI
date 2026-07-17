#!/usr/bin/env python3
"""
File: config.py

Purpose:
    Centralized configuration management for the BitSight integration.

Responsibilities:
    - Define application configuration defaults.
    - Store the BitSight API key and versioned API base URL.
    - Manage optional HTTP proxy settings.
    - Manage optional Microsoft SQL Server settings.
    - Load configuration from JSON.
    - Persist configuration using atomic file replacement.
    - Normalize and validate configuration values.
    - Redact secrets for logs and diagnostic output.
    - Support configuration path overrides through environment variables.

Default BitSight API base URL:
    https://service.bitsighttech.com/v1

Configuration file resolution:
    1. Path specified by the BITSIGHT_CONFIG_PATH environment variable.
    2. ~/.bitsight/config.json

Endpoint construction example:
    endpoint_url = f"{config.base_url}/companies"

Result:
    https://service.bitsighttech.com/v1/companies

Security:
    This module can persist credentials in plain-text JSON. Production
    deployments must restrict access to the configuration file and should use
    an approved enterprise secrets-management platform where available.
"""

import json
import os
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, Dict, Optional


# ---------------------------------------------------------------------------
# BitSight API defaults
# ---------------------------------------------------------------------------

# Versioned BitSight API root.
#
# Resource paths must be appended without adding another "/v1".
# Example:
#     f"{config.base_url}/companies"
DEFAULT_BASE_URL = "https://service.bitsighttech.com/v1"

# Default timeout, in seconds, for BitSight HTTP requests.
DEFAULT_TIMEOUT = 60


# ---------------------------------------------------------------------------
# Local configuration storage defaults
# ---------------------------------------------------------------------------

# Default configuration location:
#     ~/.bitsight/config.json
DEFAULT_CONFIG_DIRNAME = ".bitsight"
DEFAULT_CONFIG_FILENAME = "config.json"

# Optional environment variable used to override the default configuration
# file path. This supports containers, CI/CD pipelines, and managed runtime
# environments where the user's home directory should not be used.
ENV_CONFIG_PATH = "BITSIGHT_CONFIG_PATH"


class ConfigError(Exception):
    """
    Raised when configuration cannot be loaded, saved, normalized, or validated.

    Lower-level exceptions are wrapped in ConfigError so calling applications
    can handle configuration failures through one stable exception type.
    """


def default_config_path() -> Path:
    """
    Resolve the effective configuration file path.

    Resolution order:
        1. BITSIGHT_CONFIG_PATH environment variable.
        2. ~/.bitsight/config.json.

    Returns:
        Expanded pathlib.Path for the configuration file.

    Notes:
        This function does not create the file or its parent directory.
        ConfigStore.save() creates the parent directory when required.
    """
    configured_path = os.environ.get(ENV_CONFIG_PATH)

    if configured_path:
        return Path(configured_path).expanduser()

    return Path.home() / DEFAULT_CONFIG_DIRNAME / DEFAULT_CONFIG_FILENAME


def _normalize_base_url(value: str) -> str:
    """
    Normalize the configured BitSight API base URL.

    Normalization rules:
        - Empty values fall back to DEFAULT_BASE_URL.
        - Leading and trailing whitespace is removed.
        - Trailing slash characters are removed.

    Args:
        value:
            User-provided or persisted base URL.

    Returns:
        Normalized base URL without a trailing slash.

    Example:
        Input:
            https://service.bitsighttech.com/v1/

        Output:
            https://service.bitsighttech.com/v1
    """
    normalized_value = (value or "").strip()

    if not normalized_value:
        return DEFAULT_BASE_URL

    return normalized_value.rstrip("/")


@dataclass(frozen=True)
class Config:
    """
    Immutable application configuration.

    The frozen dataclass prevents accidental in-place mutation after the
    configuration has been loaded. Updates are performed with
    dataclasses.replace(), which returns a new Config instance.

    Secret fields:
        - api_key
        - proxy_password
        - mssql_password
    """

    # -----------------------------------------------------------------------
    # BitSight API configuration
    # -----------------------------------------------------------------------

    # BitSight API credential.
    #
    # Authorization header construction belongs in the HTTP client and is not
    # handled by config.py.
    api_key: Optional[str] = None

    # Versioned BitSight API root.
    base_url: str = DEFAULT_BASE_URL

    # Timeout, in seconds, for BitSight HTTP requests.
    timeout: int = DEFAULT_TIMEOUT

    # -----------------------------------------------------------------------
    # HTTP proxy configuration
    # -----------------------------------------------------------------------

    # Proxy URL used for both HTTP and HTTPS requests.
    proxy_url: Optional[str] = None

    # Optional proxy credentials.
    proxy_username: Optional[str] = None
    proxy_password: Optional[str] = None

    # -----------------------------------------------------------------------
    # Microsoft SQL Server configuration
    # -----------------------------------------------------------------------

    mssql_server: Optional[str] = None
    mssql_database: Optional[str] = None
    mssql_username: Optional[str] = None
    mssql_password: Optional[str] = None

    # Default Microsoft ODBC driver expected by the database integration.
    mssql_driver: str = "ODBC Driver 18 for SQL Server"

    # Require encrypted SQL Server connections by default.
    mssql_encrypt: bool = True

    # Do not trust an unverified SQL Server certificate by default.
    mssql_trust_cert: bool = False

    # SQL Server connection timeout, in seconds.
    mssql_timeout: int = 30

    def to_dict(self, include_secrets: bool = True) -> Dict[str, Any]:
        """
        Convert the configuration to a JSON-serializable dictionary.

        Args:
            include_secrets:
                When True, include actual secret values for persistence.
                When False, replace populated secrets with "***".

        Returns:
            Dictionary containing all configuration fields.

        Security:
            Always use include_secrets=False for logs, telemetry, exceptions,
            debug output, or user-visible diagnostics.
        """
        data = asdict(self)

        if not include_secrets:
            for field_name in (
                "api_key",
                "proxy_password",
                "mssql_password",
            ):
                if data.get(field_name):
                    data[field_name] = "***"

        return data

    def proxies(self) -> Optional[Dict[str, str]]:
        """
        Build a requests-compatible proxy dictionary.

        Returns:
            Dictionary containing "http" and "https" proxy entries, or None
            when no proxy URL is configured.

        Example:
            {
                "http": "https://proxy.example.com:8443",
                "https": "https://proxy.example.com:8443"
            }
        """
        if not self.proxy_url:
            return None

        normalized_proxy_url = self.proxy_url.strip()

        if not normalized_proxy_url:
            return None

        return {
            "http": normalized_proxy_url,
            "https": normalized_proxy_url,
        }

    def validate(self, require_api_key: bool = False) -> None:
        """
        Validate the current configuration.

        Args:
            require_api_key:
                When True, validation fails unless a non-empty BitSight API key
                is configured.

        Raises:
            ConfigError:
                If a required value is missing or a configured value is invalid.

        Notes:
            Full SQL Server validation is deferred until database functionality
            is used. This allows API-only deployments to operate without SQL
            Server configuration.
        """
        if require_api_key and not (self.api_key and self.api_key.strip()):
            raise ConfigError("Missing api_key")

        if self.timeout <= 0:
            raise ConfigError("timeout must be greater than 0")

        if self.mssql_timeout <= 0:
            raise ConfigError("mssql_timeout must be greater than 0")

        normalized_base_url = _normalize_base_url(self.base_url)

        if not normalized_base_url.startswith(("http://", "https://")):
            raise ConfigError(
                "base_url must start with http:// or https://"
            )

        if self.proxy_url is not None:
            normalized_proxy_url = self.proxy_url.strip()

            if normalized_proxy_url and not normalized_proxy_url.startswith(
                ("http://", "https://")
            ):
                raise ConfigError(
                    "proxy_url must start with http:// or https://"
                )


class ConfigStore:
    """
    JSON-backed configuration repository.

    ConfigStore is responsible for:
        - Resolving the configuration path.
        - Loading configuration from disk.
        - Ignoring unknown JSON keys for compatibility.
        - Normalizing and validating loaded values.
        - Atomically saving configuration updates.
        - Resetting configuration to defaults.
        - Clearing persisted secret values.
    """

    def __init__(self, path: Optional[str] = None):
        """
        Initialize the configuration repository.

        Args:
            path:
                Optional explicit configuration path. When omitted, the path is
                resolved by default_config_path().
        """
        self.path = (
            Path(path).expanduser()
            if path
            else default_config_path()
        )

    def exists(self) -> bool:
        """
        Return True when the configuration file exists.
        """
        return self.path.exists()

    def load(self) -> Config:
        """
        Load and validate configuration from disk.

        Returns:
            Validated Config instance.

        Behavior:
            - Returns default configuration when the file does not exist.
            - Treats an empty file as an empty JSON object.
            - Ignores unknown JSON properties.
            - Normalizes the BitSight base URL.
            - Converts timeout fields to integers.

        Raises:
            ConfigError:
                If the file cannot be read, contains invalid JSON, has an
                invalid root type, or contains invalid configuration values.
        """
        if not self.path.exists():
            return Config()

        try:
            raw_content = self.path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ConfigError(
                f"Unable to read config: {self.path} ({exc})"
            ) from exc

        try:
            parsed_data = (
                json.loads(raw_content)
                if raw_content.strip()
                else {}
            )
        except json.JSONDecodeError as exc:
            raise ConfigError(
                f"Invalid JSON in config: {self.path} ({exc})"
            ) from exc

        if not isinstance(parsed_data, dict):
            raise ConfigError("Config root must be a JSON object")

        # Load only fields defined by the current Config dataclass.
        #
        # Unknown keys are intentionally ignored to provide limited backward
        # and forward compatibility between application versions.
        config_values: Dict[str, Any] = {}

        for field_name in Config.__dataclass_fields__:
            if field_name in parsed_data:
                config_values[field_name] = parsed_data[field_name]

        if "base_url" in config_values:
            config_values["base_url"] = _normalize_base_url(
                str(config_values["base_url"])
            )

        # Normalize numeric values that may have been stored as JSON strings.
        try:
            if "timeout" in config_values:
                config_values["timeout"] = int(
                    config_values["timeout"]
                )

            if "mssql_timeout" in config_values:
                config_values["mssql_timeout"] = int(
                    config_values["mssql_timeout"]
                )
        except (TypeError, ValueError) as exc:
            raise ConfigError(
                "timeout and mssql_timeout must be integers"
            ) from exc

        try:
            config = Config(**config_values)
        except TypeError as exc:
            raise ConfigError(
                f"Invalid configuration: {exc}"
            ) from exc

        config.validate(require_api_key=False)
        return config

    def save(self, config: Config) -> None:
        """
        Validate and atomically persist configuration.

        The configuration is written to a temporary file in the same directory,
        then moved over the destination with os.replace(). On supported local
        filesystems, this prevents readers from observing a partially written
        JSON document.

        Args:
            config:
                Config instance to persist.

        Raises:
            ConfigError:
                If validation fails, the parent directory cannot be created,
                or the configuration cannot be written.
        """
        normalized_config = replace(
            config,
            base_url=_normalize_base_url(config.base_url),
        )

        normalized_config.validate(require_api_key=False)

        try:
            self.path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )
        except OSError as exc:
            raise ConfigError(
                "Unable to create config directory: "
                f"{self.path.parent} ({exc})"
            ) from exc

        payload = json.dumps(
            normalized_config.to_dict(include_secrets=True),
            indent=2,
            sort_keys=True,
        )

        temporary_path = self.path.with_suffix(
            self.path.suffix + ".tmp"
        )

        try:
            temporary_path.write_text(
                payload + "\n",
                encoding="utf-8",
            )

            # Replace the destination only after the complete temporary file
            # has been written successfully.
            os.replace(temporary_path, self.path)

        except OSError as exc:
            # Best-effort cleanup. Cleanup failures must not hide the original
            # persistence error.
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass

            raise ConfigError(
                f"Unable to write config: {self.path} ({exc})"
            ) from exc

    def reset(self) -> Config:
        """
        Replace the persisted configuration with application defaults.

        Returns:
            Default Config instance that was saved.
        """
        config = Config()
        self.save(config)
        return config

    def clear_keys(self) -> Config:
        """
        Clear persisted secret values while retaining non-secret settings.

        Cleared fields:
            - api_key
            - proxy_password
            - mssql_password

        Returns:
            Updated Config instance.
        """
        config = self.load()

        sanitized_config = replace(
            config,
            api_key=None,
            proxy_password=None,
            mssql_password=None,
        )

        self.save(sanitized_config)
        return sanitized_config

    def set_fields(self, **updates: Any) -> Config:
        """
        Update selected configuration fields and persist the result.

        Args:
            **updates:
                Config field names and replacement values.

                Values set to None are ignored so callers can omit optional
                inputs without unintentionally clearing existing settings.

        Returns:
            Updated and persisted Config instance.

        Raises:
            ConfigError:
                If an unknown field is supplied or a timeout value cannot be
                converted to an integer.

        Notes:
            Use clear_keys() to explicitly clear supported secret fields.
        """
        config = self.load()
        allowed_fields = set(Config.__dataclass_fields__)

        unknown_fields = set(updates) - allowed_fields

        if unknown_fields:
            field_names = ", ".join(sorted(unknown_fields))
            raise ConfigError(
                f"Unknown config field(s): {field_names}"
            )

        # Ignore None values so partial updates preserve existing values.
        normalized_updates = {
            field_name: value
            for field_name, value in updates.items()
            if value is not None
        }

        if "base_url" in normalized_updates:
            normalized_updates["base_url"] = _normalize_base_url(
                str(normalized_updates["base_url"])
            )

        for timeout_field in ("timeout", "mssql_timeout"):
            if timeout_field not in normalized_updates:
                continue

            try:
                normalized_updates[timeout_field] = int(
                    normalized_updates[timeout_field]
                )
            except (TypeError, ValueError) as exc:
                raise ConfigError(
                    f"{timeout_field} must be an integer"
                ) from exc

        updated_config = replace(
            config,
            **normalized_updates,
        )

        self.save(updated_config)
        return updated_config
