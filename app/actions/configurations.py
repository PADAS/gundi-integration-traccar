from enum import Enum
from pydantic import Field

from app.services.errors import ConfigurationNotFound
from app.services.utils import find_config_for_action
from .core import PullActionConfiguration, AuthActionConfiguration


class AuthenticateConfig(AuthActionConfiguration):
    email: str
    password: str


class FetchSamplesConfig(PullActionConfiguration):
    observations_to_extract: int = 20


class RecordedAtFieldNameEnum(str, Enum):
    fixTime = 'fixTime'
    serverTime = 'serverTime'


class PullObservationsConfig(PullActionConfiguration):
    recorded_at_field_name: RecordedAtFieldNameEnum = Field(
        RecordedAtFieldNameEnum.fixTime,
        description="The integration take this value from observations as 'recorded_at' value"
    )


class PullObservationsPerDeviceConfig(PullActionConfiguration):
    device_id: str
    device_name: str
    recorded_at_field_name: str
    observations_per_request: int = 500


def get_auth_config(integration):
    # Look for the login credentials, needed for any action
    auth_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="auth"
    )
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return AuthenticateConfig.parse_obj(auth_config.data)


def get_fetch_samples_config(integration):
    # Look for the login credentials, needed for any action
    fetch_samples_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="fetch_samples"
    )
    if not fetch_samples_config:
        raise ConfigurationNotFound(
            f"fetch_samples settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return FetchSamplesConfig.parse_obj(fetch_samples_config.data)


def get_pull_observations_config(integration):
    # Look for the login credentials, needed for any action
    pull_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="pull_observations"
    )
    if not pull_config:
        raise ConfigurationNotFound(
            f"pull_observations settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return PullObservationsConfig.parse_obj(pull_config.data)
