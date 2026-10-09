"""GHMQ: explicit, outbound-only GitHub event actions."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass

import probatio
from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.const import EVENT_HOMEASSISTANT_STOP
from homeassistant.core import HomeAssistant, ServiceCall, SupportsResponse
from homeassistant.exceptions import (
    ConfigEntryAuthFailed,
    ConfigEntryNotReady,
    HomeAssistantError,
    ServiceValidationError,
)
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import (
    CONF_BRANCH,
    CONF_ENABLED,
    CONF_OWNER,
    CONF_PR_NUMBER,
    CONF_PULL_REQUEST_ID,
    CONF_REPOSITORY,
    CONF_REPOSITORY_ID,
    CONF_TOKEN,
    DOMAIN,
)
from .core import AuthError, BridgeError, JournalError, Sender, SenderClosed
from .github import GitHubClient
from .journal import Journal


@dataclass
class Runtime:
    sender: Sender
    enabled: bool


def make_client(hass: HomeAssistant, data: dict) -> GitHubClient:
    return GitHubClient(
        async_get_clientsession(hass),
        data[CONF_OWNER],
        data[CONF_REPOSITORY],
        data[CONF_BRANCH],
        data[CONF_PR_NUMBER],
        data[CONF_TOKEN],
        repository_id=data.get(CONF_REPOSITORY_ID),
        pull_request_id=data.get(CONF_PULL_REQUEST_ID),
    )


def bind_journal(client: GitHubClient, journal: Journal) -> None:
    client.bind_cooldown(
        journal.get_cooldown_deadline(),
        journal.async_set_cooldown,
        current_deadline=journal.get_cooldown_deadline,
        announce_deadline=journal.note_cooldown,
        is_cooldown_pending=journal.is_cooldown_pending,
    )


async def async_validate_config(hass, data, *, entry=None, discover=False):
    """Read-only validation with persistent cooldown and entry lifecycle ownership."""
    runtime = getattr(entry, "runtime_data", None) if entry is not None else None
    if runtime is not None:
        # A late reauth response must never outlive unload and overwrite a new
        # sender's state. Validation shares and drains the sender serializer.
        async with runtime.sender.lock:
            runtime.sender._check_open()
            client = make_client(hass, data)
            bind_journal(client, runtime.sender.store)
            identities = await client.validate_destination(discover=discover)
            runtime.sender._check_open()
            return identities
    if entry is not None:
        key = entry.entry_id
    else:
        destination = "\0".join(
            str(data.get(k, ""))
            for k in (CONF_OWNER, CONF_REPOSITORY, CONF_BRANCH, CONF_PR_NUMBER)
        )
        key = "setup_" + hashlib.sha256(destination.lower().encode()).hexdigest()[:32]
    journal = Journal.for_hass(hass, key)
    await journal.async_load()
    client = make_client(hass, data)
    bind_journal(client, journal)
    return await client.validate_destination(discover=discover)


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    async def send_event(call: ServiceCall):
        data = dict(call.data)
        entry_id = data.pop("config_entry_id", None)
        entries = [
            entry
            for entry in hass.config_entries.async_entries(DOMAIN)
            if entry.state is ConfigEntryState.LOADED
            and getattr(entry, "runtime_data", None) is not None
        ]
        entry = (
            next((entry for entry in entries if entry.entry_id == entry_id), None)
            if entry_id
            else (entries[0] if len(entries) == 1 else None)
        )
        if entry is None:
            raise ServiceValidationError(
                "Select one configured, loaded GHMQ integration"
            )
        runtime = entry.runtime_data
        if not runtime.enabled:
            raise ServiceValidationError(
                "Sending is disabled; enable it in the integration options after reviewing the destination"
            )
        try:
            return await runtime.sender.send(data)
        except AuthError:
            entry.async_start_reauth(hass)
            raise HomeAssistantError(
                "GitHub authentication failed; reconnect in the integration settings"
            ) from None
        except BridgeError as err:
            raise HomeAssistantError(str(err)) from None
        except OSError:
            raise HomeAssistantError(
                "Could not save the relay journal; delivery is not confirmed"
            ) from None

    # Validation is also performed in the independently tested core. Disallow
    # arbitrary data, per-call recipients and credentials at the action boundary.
    schema = probatio.Schema(
        {
            probatio.Optional("config_entry_id"): str,
            probatio.Optional("event_id"): str,
            probatio.Required("event_type"): str,
            probatio.Required("message"): str,
            probatio.Optional("severity", default="info"): str,
            probatio.Optional("synthetic", default=False): bool,
            probatio.Optional("entity_id"): str,
            probatio.Optional("occurred_at"): str,
            probatio.Optional("ttl_seconds", default=300): int,
        }
    )
    hass.services.async_register(
        DOMAIN,
        "send_event",
        send_event,
        schema=schema,
        supports_response=SupportsResponse.OPTIONAL,
    )
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    try:
        client = make_client(hass, entry.data)
        journal = Journal.for_hass(hass, entry.entry_id)
        sender = Sender(
            client,
            journal,
            create_task=lambda coro: entry.async_create_task(
                hass, coro, "ghmq-journal-commit", eager_start=False
            ),
        )
        await sender.load()
        bind_journal(client, journal)
        await client.validate_destination()
    except AuthError as err:
        raise ConfigEntryAuthFailed(str(err)) from None
    except (BridgeError, OSError):
        raise ConfigEntryNotReady(
            "Cannot initialize private GitHub relay; check destination and journal"
        ) from None
    enabled = entry.options.get(CONF_ENABLED, False)
    runtime = Runtime(sender, enabled)
    entry.runtime_data = runtime

    async def stop_sender(_event):
        runtime.enabled = False
        await sender.async_close()

    entry.async_on_unload(hass.bus.async_listen(EVENT_HOMEASSISTANT_STOP, stop_sender))

    async def retry_loop():
        while True:
            try:
                await sender.retry_pending()
            except AuthError:
                entry.async_start_reauth(hass)
                return
            except (JournalError, SenderClosed):
                return
            except (BridgeError, OSError):
                sender.last_status = "retry_pending"
            await asyncio.sleep(30)

    if enabled:
        entry.async_create_background_task(hass, retry_loop(), "ghmq-pending-delivery")
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    # Service calls are not entry-owned background tasks. Stop admission and
    # drain their journal/network work before HA cancels the retry task and
    # removes runtime_data after successful unload.
    runtime = getattr(entry, "runtime_data", None)
    if runtime is not None:
        runtime.enabled = False
        await runtime.sender.async_close()
    return True
