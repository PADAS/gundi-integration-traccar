import httpx
import backoff
import logging

from enum import Enum
from datetime import datetime, timezone
from pydantic import BaseModel, Field
from typing import List, Optional, Any

from app.actions.configurations import get_auth_config
from app.services.state import IntegrationStateManager


state_manager = IntegrationStateManager()
logger = logging.getLogger(__name__)


class RecordedAtFieldEnum(str, Enum):
    fixTime = 'fixTime'
    serverTime = 'serverTime'


class DeviceState(BaseModel):
    recorded_at: datetime
    recorded_at_field_name: Optional[RecordedAtFieldEnum] = RecordedAtFieldEnum.fixTime


class PositionAttributes(BaseModel):
    battery: Optional[float]
    distance: Optional[float]
    ip: Optional[str]
    totalDistance: Optional[float]


class TraccarPosition(BaseModel):
    deviceTime: datetime
    latitude: float
    longitude: float
    deviceId: int
    accuracy: Optional[float]
    address: Optional[Any]
    altitude: Optional[float]
    attributes: Optional[PositionAttributes]
    course: Optional[float]
    fixTime: Optional[datetime]
    id: Optional[int]
    network: Optional[Any]
    outdated: Optional[bool]
    protocol: Optional[str]
    serverTime: Optional[datetime]
    speed: Optional[float]
    type: Optional[Any]
    valid: Optional[bool]


@backoff.on_exception(backoff.expo, (httpx.TimeoutException, httpx.HTTPStatusError, httpx.ConnectError), max_time=30)
async def get_device_list(integration):
    url = integration.base_url + '/devices'
    auth = get_auth_config(integration)
    try:
        async with httpx.AsyncClient(timeout=120) as session:
            response = await session.get(
                url,
                auth=(auth.email, auth.password),
            )
            response.raise_for_status()
            response = response.json()
        device_ids = [(str(d.get('id')), d.get('name')) for d in response]
        return device_ids
    except httpx.HTTPError as e:
        logger.exception(
            f'Exception raised while getting device list from traccar api',
             extra={
                 'url': '/devices',
                 'needs_attention': True
             }
        )
        raise e


def generate_positions(positions: List[TraccarPosition], lower_date=None, recorded_at_field_name=None) -> List[TraccarPosition]:
    '''
    Generate positions from the list of positions
    This is for the case where we get several records for a single "fix" indicated by fixTime.
    The goal is to return the first reported fixTime; we assume deviceTime is how we determine that.
    So for example, if we a result that includes 30 records for a fix at 2024-02-03 12:01:01Z we'll filter
    the list to just the first occurrence.

    :param: positions: list of positions
    :param: lower_date: optional lower-date to use as a filter (ex. get only positions after the latest timestamp we know about).
    '''
    time_cursor = lower_date or datetime.min.replace(tzinfo=timezone.utc)
    for position in sorted(positions, key=lambda x: (getattr(x, recorded_at_field_name), x.deviceTime)):
        if getattr(position, recorded_at_field_name) > time_cursor:
            yield position
            time_cursor = getattr(position, recorded_at_field_name)


@backoff.on_exception(backoff.expo, (httpx.TimeoutException, httpx.HTTPStatusError, httpx.ConnectError), max_time=30)
async def get_positions_since(integration, lower_date: datetime, device_id: str, recorded_at_field_name):
    url = integration.base_url + '/positions'
    auth = get_auth_config(integration)
    try:
        params = {
            'deviceId': device_id,
            'from': lower_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'to': datetime.now(tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        }

        logger.info(f'Requesting data for: params', extra={'params': params, 'url': '/positions'})

        async with httpx.AsyncClient(timeout=120) as session:
            response = await session.get(
                url,
                auth=(auth.email, auth.password),
                params=params
            )
            response.raise_for_status()

        if response:
            positions = list(
                generate_positions(
                    [TraccarPosition(**p) for p in response.json()],
                    lower_date=lower_date,
                    recorded_at_field_name=recorded_at_field_name
                )
            )
        else:
            positions = []
    except httpx.HTTPError as e:
        logger.error(
            f'Exception {e} occurred while getting positions',
            extra={
                'url': '/positions',
                'params': params,
                'needs_attention': True
            }
        )
    else:
        return positions
