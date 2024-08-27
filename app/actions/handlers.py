import datetime
import httpx
import logging
import stamina
import pydantic
import redis.exceptions
import app.actions.client as client

from app.actions.configurations import AuthenticateConfig, FetchSamplesConfig, PullObservationsConfig
from app.services.activity_logger import activity_logger
from app.services.gundi import send_observations_to_gundi
from app.services.state import IntegrationStateManager


logger = logging.getLogger(__name__)


state_manager = IntegrationStateManager()


async def transform(position: client.TraccarPosition, device_id: str, device_name: str, recorded_at_fn=lambda x: x.fixTime):
    return {
        "source": device_id,
        "source_name": device_name,
        'type': 'tracking-device',
        "recorded_at": recorded_at_fn(position),
        "location": {
            "lat": position.latitude,
            "lon": position.longitude
        },
        "additional": dict(
            accuracy=position.accuracy,
            address=position.address,
            altitude=position.altitude,
            radio_type=position.network.get('radioType', None) if position.network else None,
            course=position.course,
            id=position.id,
            protocol=position.protocol,
            serverTime=position.serverTime,
            fixTime = position.fixTime,
            speed=position.speed,
        )
    }


async def action_auth(integration, action_config: AuthenticateConfig):
    logger.info(f"Executing auth action with integration {integration} and action_config {action_config}...")
    try:
        response = await client.get_device_list(
            integration=integration
        )
    except httpx.HTTPError as e:
        message = f"auth action returned error."
        logger.exception(message, extra={
            "integration_id": str(integration.id),
            "attention_needed": True
        })
        raise e
    else:
        logger.info(f"Authenticated with success.")
        return {"valid_credentials": response is not None}


async def action_fetch_samples(integration, action_config: FetchSamplesConfig):
    logger.info(f"Executing fetch_samples action with integration {integration} and action_config {action_config}...")
    try:
        devices = await client.get_device_list(
            integration=integration
        )
    except httpx.HTTPError as e:
        message = f"fetch_samples action returned error."
        logger.exception(message, extra={
            "integration_id": str(integration.id),
            "attention_needed": True
        })
        raise e
    else:
        logger.info(f"Observations pulled with success.")
        return {
            "devices_extracted": action_config.observations_to_extract,
            "devices": devices[:action_config.observations_to_extract]
        }


@activity_logger()
async def action_pull_observations(integration, action_config: PullObservationsConfig):
    logger.info(f"Executing pull_observations action with integration {integration} and action_config {action_config}...")
    async for attempt in stamina.retry_context(
            on=httpx.HTTPError,
            attempts=3,
            wait_initial=datetime.timedelta(seconds=10),
            wait_max=datetime.timedelta(seconds=10),
    ):
        with attempt:
            devices = await client.get_device_list(
                integration=integration
            )

    if devices:
        logger.info(f"Devices pulled with success. Length: {len(devices)}")

        start_time_limit = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1)
        device_count = len(devices)

        for i, device in enumerate(devices):
            logger.info(f'Processing device {device[0]} ({i + 1}/{device_count} for integration {integration.name})')

            device_id = device[0]
            device_name = device[1]

            try:
                device_state = await state_manager.get_state(
                    str(integration.id),
                    "pull_observations",
                    device_id
                )
                if device_state:
                    device_state = client.DeviceState.parse_obj(device_state)
                    # Assign recorded_at_field_name value from config to current state
                    device_state.recorded_at_field_name = action_config.recorded_at_field_name.value
                else:
                    device_state = client.DeviceState(
                        recorded_at=start_time_limit,
                        recorded_at_field_name=action_config.recorded_at_field_name.value
                    )
            except pydantic.ValidationError as e:
                logger.info(f"Invalid device state for device {device_id}. Exception: {e}")
                device_state = client.DeviceState(
                    recorded_at=start_time_limit,
                    recorded_at_field_name=action_config.recorded_at_field_name.value
                )
            except redis.exceptions.RedisError as e:
                logger.info(f"Error while reading device state from cache. Device {device_id}. Exception: {e}")
                device_state = client.DeviceState(
                    recorded_at=start_time_limit,
                    recorded_at_field_name=action_config.recorded_at_field_name.value
                )

            # Ensure we don't go back further than 24 hours
            start_at = max(device_state.recorded_at, start_time_limit)

            traccar_observations = await client.get_positions_since(
                integration, start_at, device_id, device_state.recorded_at_field_name
            )

            logger.info(
                f'{len(traccar_observations)} observations found for device {device_id} ({device_name})'
            )

            if traccar_observations:
                def fn(p): return getattr(p, device_state.recorded_at_field_name)

                transformed_data = list(
                    [await transform(p, device_id, device_name, recorded_at_fn=fn) for p in traccar_observations]
                )

                if transformed_data:
                    def generate_batches(iterable, n=action_config.observations_per_request):
                        for i in range(0, len(iterable), n):
                            yield iterable[i: i + n]

                    for i, batch in enumerate(generate_batches(transformed_data)):
                        async for attempt in stamina.retry_context(
                                on=httpx.HTTPError,
                                attempts=3,
                                wait_initial=datetime.timedelta(seconds=10),
                                wait_max=datetime.timedelta(seconds=10),
                        ):
                            with attempt:
                                try:
                                    logger.info(
                                        f'Sending observations batch #{i}: {len(batch)} observations. Device {device_id}'
                                    )
                                    await send_observations_to_gundi(
                                        observations=batch,
                                        integration_id=str(integration.id)
                                    )
                                except httpx.HTTPError as e:
                                    msg = f'Sensors API returned error for integration_id: {str(integration.id)}. Exception: {e}'
                                    logger.exception(
                                        msg,
                                        extra={
                                            'needs_attention': True,
                                            'integration_id': str(integration.id),
                                            'action_id': "pull_observations"
                                        }
                                    )
                                    raise e
                                else:
                                    for vehicle in batch:
                                        # Update state
                                        state = {
                                            "recorded_at": vehicle.get("recorded_at")
                                        }
                                        await state_manager.set_state(
                                            str(integration.id),
                                            "pull_observations",
                                            state,
                                            vehicle.get("source")
                                        )

            else:
                logger.info(f"No observation extracted for device: {device_id}.")

    return
