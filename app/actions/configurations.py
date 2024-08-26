from app.services.errors import ConfigurationNotFound
from app.services.utils import find_config_for_action
from .core import PullActionConfiguration, AuthActionConfiguration


class AuthenticateConfig(AuthActionConfiguration):
    email: str
    password: str


class FetchSamplesConfig(PullActionConfiguration):
    observations_to_extract: int = 20


class PullObservationsConfig(PullActionConfiguration):
    # We may include something here in the future
    pass


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


def get_pull_config(integration):
    # Look for the login credentials, needed for any action
    pull_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="pull_observations"
    )
    if not pull_config:
        raise ConfigurationNotFound(
            f"pull_config settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return PullObservationsConfig.parse_obj(pull_config.data)
