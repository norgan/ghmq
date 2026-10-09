"""English UI and config-flow contracts against real Home Assistant APIs.

Only GitHub transport/validation and reload scheduling are intercepted. No HTTP request,
GitHub mutation, or running Home Assistant instance is needed by these tests.
"""

from __future__ import annotations

import importlib.util
import json
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

try:
    import probatio
    from homeassistant import config_entries, loader
    from homeassistant.data_entry_flow import InvalidData
    from homeassistant.helpers import selector, service, translation
except ModuleNotFoundError as error:
    if importlib.util.find_spec("homeassistant") is None:
        raise unittest.SkipTest(
            "Real Home Assistant dependencies are not installed"
        ) from error
    raise

from custom_components.ghmq import config_flow
from custom_components.ghmq.const import CONF_ENABLED, DOMAIN
from custom_components.ghmq.core import (
    AuthError,
    BridgeError,
    IdentityError,
    JournalError,
    RateLimitError,
    RetryableError,
)
from tests import test_ha_adapter as fixtures

INTEGRATION = Path(__file__).resolve().parents[1] / "custom_components" / DOMAIN
USER_DATA = {
    "owner": "example-owner",
    "repository": "ghmq",
    "branch": "relay/events",
    "pull_request_number": 1,
    "token": "inert-original-token",
}
IDENTITY = {"repository_id": 101, "pull_request_id": 1001}
FIELDS = {
    "config_entry_id",
    "event_type",
    "message",
    "event_id",
    "occurred_at",
    "ttl_seconds",
    "severity",
    "synthetic",
    "entity_id",
}


class RealHomeAssistantUITests(unittest.IsolatedAsyncioTestCase):
    # Reuse the real-HA lifecycle and the HTTP tripwire without inheriting or
    # rerunning the adapter test methods themselves.
    asyncSetUp = fixtures.RealHomeAssistantTests.asyncSetUp
    asyncTearDown = fixtures.RealHomeAssistantTests.asyncTearDown
    new_entry = fixtures.RealHomeAssistantTests.new_entry

    def pinned_entry(self, *, enabled=False):
        entry = self.new_entry(enabled=enabled)
        self.hass.config_entries.async_update_entry(
            entry, data={**USER_DATA, **IDENTITY}
        )
        return entry

    async def start_reauth(self, entry):
        result = await self.hass.config_entries.flow.async_init(
            DOMAIN,
            context={
                "source": config_entries.SOURCE_REAUTH,
                "entry_id": entry.entry_id,
                "unique_id": entry.unique_id,
            },
            data=dict(entry.data),
        )
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "reauth_confirm")
        return result

    async def test_pr_selector_has_minimum_and_invalid_values_never_validate(self):
        flow = config_flow.ConfigFlow()
        flow.hass = self.hass
        flow.context = {"source": config_entries.SOURCE_USER}
        form = await flow.async_step_user()
        number = next(
            value
            for key, value in form["data_schema"].schema.items()
            if str(key) == "pull_request_number"
        )
        self.assertEqual(number.config["min"], 1)
        self.assertEqual(number.config["step"], 1)
        self.assertEqual(number.config["mode"], "box")
        for invalid in (0, -1, 1.5, True, False, None, "1", float("nan"), float("inf")):
            with (
                self.subTest(schema_value=invalid),
                self.assertRaises(probatio.Invalid),
            ):
                form["data_schema"]({**USER_DATA, "pull_request_number": invalid})
        self.assertEqual(number(9007199254740993), 9007199254740993)
        for value in (0, -1, 1.5, True, False, None, "1", float("nan"), float("inf")):
            with (
                self.subTest(value=value),
                patch.object(
                    config_flow, "async_validate_config", new=AsyncMock()
                ) as validate,
            ):
                result = await flow.async_step_user(
                    {**USER_DATA, "pull_request_number": value}
                )
                self.assertEqual(
                    result["errors"],
                    {"pull_request_number": "invalid_pull_request_number"},
                )
                validate.assert_not_awaited()

    async def test_actual_flow_manager_rejects_boolean_pr_before_network(self):
        form = await self.hass.config_entries.flow.async_init(
            DOMAIN, context={"source": config_entries.SOURCE_USER}
        )
        with patch.object(
            config_flow, "async_validate_config", new=AsyncMock()
        ) as validate:
            for value in (True, False, 0, 1.5):
                with self.subTest(value=value), self.assertRaises(InvalidData) as error:
                    await self.hass.config_entries.flow.async_configure(
                        form["flow_id"], {**USER_DATA, "pull_request_number": value}
                    )
                self.assertEqual(error.exception.path, ["pull_request_number"])
            validate.assert_not_awaited()
        self.hass.config_entries.flow.async_abort(form["flow_id"])

    async def test_pr_selector_float_is_normalized_before_validation_and_storage(self):
        flow = config_flow.ConfigFlow()
        flow.hass = self.hass
        flow.context = {"source": config_entries.SOURCE_USER}
        form = await flow.async_step_user()
        validated = form["data_schema"]({**USER_DATA, "pull_request_number": 1.0})
        self.assertIs(type(validated["pull_request_number"]), int)
        with patch.object(
            config_flow, "async_validate_config", new=AsyncMock(return_value=IDENTITY)
        ) as validate:
            result = await flow.async_step_user(validated)
        self.assertIs(type(validate.call_args.args[1]["pull_request_number"]), int)
        self.assertIs(type(result["data"]["pull_request_number"]), int)

    async def test_config_failure_logs_only_allowlisted_categories(self):
        secret = "private-token-body-url-header-detail"
        for error, category in (
            (AuthError(secret), "authentication"),
            (IdentityError(secret), "identity"),
            (JournalError(secret), "journal"),
            (RetryableError(), "retry_or_cooldown"),
            (RateLimitError(60, 2000000000), "rate_limited"),
            (BridgeError(secret), "validation"),
            (BridgeError("Invalid GitHub owner"), "invalid_owner"),
            (BridgeError("GitHub returned an invalid response"), "invalid_response"),
        ):
            with self.subTest(category=category):
                flow = config_flow.ConfigFlow()
                flow.hass = self.hass
                flow.context = {"source": config_entries.SOURCE_USER}
                with (
                    patch.object(
                        config_flow,
                        "async_validate_config",
                        new=AsyncMock(side_effect=error),
                    ),
                    self.assertLogs(config_flow.__name__, level="WARNING") as logs,
                ):
                    result = await flow.async_step_user({**USER_DATA, "token": secret})
                self.assertEqual(result["type"], "form")
                self.assertEqual(
                    [record.getMessage() for record in logs.records],
                    [f"GHMQ validation failed: category={category}"],
                )
                self.assertTrue(all(record.exc_info is None for record in logs.records))
                self.assertNotIn(secret, " ".join(logs.output))

    async def test_setup_and_real_reauth_show_retry_deadline_without_changing_token(
        self,
    ):
        deadline = 2000000000
        expected = "2033-05-18 03:33:20 UTC"
        flow = config_flow.ConfigFlow()
        flow.hass = self.hass
        flow.context = {"source": config_entries.SOURCE_USER}
        with patch.object(
            config_flow,
            "async_validate_config",
            new=AsyncMock(side_effect=RateLimitError(60, deadline)),
        ):
            result = await flow.async_step_user(USER_DATA.copy())
        self.assertEqual(result["errors"], {"base": "rate_limited"})
        self.assertEqual(result["description_placeholders"]["retry_at"], expected)
        entry = self.pinned_entry()
        before = dict(entry.data)
        form = await self.start_reauth(entry)
        with (
            patch.object(
                config_flow,
                "async_validate_config",
                new=AsyncMock(side_effect=RateLimitError(60, deadline)),
            ),
            patch.object(self.hass.config_entries, "async_schedule_reload") as reload,
        ):
            result = await self.hass.config_entries.flow.async_configure(
                form["flow_id"], {"token": "inert-new-token"}
            )
        self.assertEqual(result["errors"], {"base": "rate_limited"})
        self.assertEqual(result["description_placeholders"]["retry_at"], expected)
        self.assertEqual(dict(entry.data), before)
        reload.assert_not_called()
        self.hass.config_entries.flow.async_abort(form["flow_id"])

    async def test_unrepresentable_deadline_has_safe_ui_fallback(self):
        self.assertEqual(
            config_flow.retry_deadline(RateLimitError(60, 1e300)),
            "a future GitHub reset time",
        )

    async def test_creation_pins_validated_ids_and_starts_disabled(self):
        flow = config_flow.ConfigFlow()
        flow.hass = self.hass
        flow.context = {"source": config_entries.SOURCE_USER}
        with patch.object(
            config_flow,
            "async_validate_config",
            new=AsyncMock(return_value=IDENTITY),
        ) as validate:
            result = await flow.async_step_user(USER_DATA.copy())
        validate.assert_awaited_once_with(self.hass, USER_DATA, discover=True)
        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"], {**USER_DATA, **IDENTITY})
        self.assertEqual(result["options"], {CONF_ENABLED: False})
        self.assertEqual(result["version"], 1)
        self.assertEqual(flow.unique_id, "example-owner/ghmq:relay/events")
        self.assertNotIn("repository_id", USER_DATA)

    async def test_user_schema_requires_explicit_destination_and_hides_token(self):
        flow = config_flow.ConfigFlow()
        flow.hass = self.hass
        flow.context = {"source": config_entries.SOURCE_USER}
        form = await flow.async_step_user()
        schema = form["data_schema"]
        self.assertEqual(schema(USER_DATA), USER_DATA)
        for required in ("owner", "repository", "pull_request_number"):
            with self.subTest(required=required), self.assertRaises(probatio.Invalid):
                schema({key: val for key, val in USER_DATA.items() if key != required})
        for forbidden in ("repository_id", "pull_request_id", "sending_enabled"):
            with self.subTest(forbidden=forbidden), self.assertRaises(probatio.Invalid):
                schema({**USER_DATA, forbidden: 99})
        token = next(
            value for key, value in schema.schema.items() if str(key) == "token"
        )
        self.assertEqual(token.config["type"], "password")

    async def test_reauth_real_flow_manager_updates_only_token_and_schedules_reload(
        self,
    ):
        for enabled in (False, True):
            with self.subTest(enabled=enabled):
                entry = self.pinned_entry(enabled=enabled)
                original = dict(entry.data)
                form = await self.start_reauth(entry)
                replacement = {"token": "inert-replacement-token"}
                with (
                    patch.object(
                        config_flow,
                        "async_validate_config",
                        new=AsyncMock(return_value=IDENTITY),
                    ) as validate,
                    patch.object(
                        self.hass.config_entries, "async_schedule_reload"
                    ) as schedule_reload,
                ):
                    result = await self.hass.config_entries.flow.async_configure(
                        form["flow_id"], replacement
                    )
                self.assertEqual(result["type"], "abort")
                self.assertEqual(result["reason"], "reauth_successful")
                validate.assert_awaited_once_with(
                    self.hass,
                    {**original, **replacement},
                    entry=entry,
                    discover=False,
                )
                schedule_reload.assert_called_once_with(entry.entry_id)
                self.assertEqual(dict(entry.data), {**original, **replacement})
                self.assertEqual(dict(entry.options), {CONF_ENABLED: enabled})
                self.assertEqual(entry.data["repository_id"], IDENTITY["repository_id"])
                self.assertEqual(
                    entry.data["pull_request_id"], IDENTITY["pull_request_id"]
                )

    async def test_reauth_errors_do_not_update_token_pins_or_options(self):
        for error, expected in (
            (AuthError("inert credential error"), "invalid_auth"),
            (BridgeError("inert connection error"), "cannot_connect"),
            (IdentityError("inert identity error"), "identity_changed"),
        ):
            with self.subTest(expected=expected):
                entry = self.pinned_entry(enabled=True)
                original = dict(entry.data)
                form = await self.start_reauth(entry)
                with (
                    patch.object(
                        config_flow,
                        "async_validate_config",
                        new=AsyncMock(side_effect=error),
                    ) as validate,
                    patch.object(
                        self.hass.config_entries, "async_schedule_reload"
                    ) as schedule_reload,
                ):
                    result = await self.hass.config_entries.flow.async_configure(
                        form["flow_id"], {"token": "inert-replacement-token"}
                    )
                self.assertEqual(result["type"], "form")
                self.assertEqual(result["errors"], {"base": expected})
                self.assertFalse(validate.call_args.kwargs["discover"])
                self.assertIs(validate.call_args.kwargs["entry"], entry)
                schedule_reload.assert_not_called()
                self.assertEqual(dict(entry.data), original)
                self.assertEqual(dict(entry.options), {CONF_ENABLED: True})
                self.hass.config_entries.flow.async_abort(form["flow_id"])

    async def test_reauth_never_repins_from_validation_return(self):
        entry = self.pinned_entry()
        original = dict(entry.data)
        form = await self.start_reauth(entry)
        with (
            patch.object(
                config_flow,
                "async_validate_config",
                new=AsyncMock(
                    return_value={"repository_id": 999, "pull_request_id": 9999}
                ),
            ),
            patch.object(self.hass.config_entries, "async_schedule_reload"),
        ):
            await self.hass.config_entries.flow.async_configure(
                form["flow_id"], {"token": "inert-replacement-token"}
            )
        self.assertEqual(entry.data["repository_id"], original["repository_id"])
        self.assertEqual(entry.data["pull_request_id"], original["pull_request_id"])

    async def test_legacy_missing_pins_reauth_fails_closed_without_http(self):
        for missing in (
            {"repository_id"},
            {"pull_request_id"},
            {"repository_id", "pull_request_id"},
        ):
            with self.subTest(missing=missing):
                entry = self.pinned_entry()
                original = {
                    key: value
                    for key, value in entry.data.items()
                    if key not in missing
                }
                self.hass.config_entries.async_update_entry(entry, data=original)
                form = await self.start_reauth(entry)
                # Use the real adapter and GitHub validation here. The shared
                # fixture raises if anything attempts external HTTP.
                with (
                    patch.object(
                        fixtures.adapter,
                        "async_get_clientsession",
                        return_value=object(),
                    ),
                    patch.object(
                        self.hass.config_entries, "async_schedule_reload"
                    ) as schedule_reload,
                ):
                    result = await self.hass.config_entries.flow.async_configure(
                        form["flow_id"], {"token": "inert-replacement-token"}
                    )
                self.assertEqual(result["type"], "form")
                self.assertEqual(result["errors"], {"base": "identity_changed"})
                schedule_reload.assert_not_called()
                self.assertEqual(dict(entry.data), original)
                self.assertEqual(entry.version, 1)
                self.assertEqual(dict(entry.options), {CONF_ENABLED: False})
                self.hass.config_entries.flow.async_abort(form["flow_id"])

    async def test_options_default_is_false_when_no_option_has_been_saved(self):
        entry = self.pinned_entry()
        self.hass.config_entries.async_update_entry(entry, options={})
        flow = config_flow.ConfigFlow.async_get_options_flow(entry)
        flow.hass = self.hass
        flow.handler = entry.entry_id
        form = await flow.async_step_init()
        self.assertFalse(form["data_schema"]({})[CONF_ENABLED])
        result = await flow.async_step_init({CONF_ENABLED: False})
        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"], {CONF_ENABLED: False})

    async def test_service_description_loads_with_all_fields_and_safe_limits(self):
        integration = await loader.async_get_integration(self.hass, DOMAIN)
        self.assertEqual(integration.name, "GHMQ")
        descriptions = await service.async_get_all_descriptions(self.hass)
        action = descriptions[DOMAIN]["send_event"]
        self.assertEqual(action["name"], "Send notification event")
        self.assertEqual(action["response"], {"optional": True})
        self.assertEqual(set(action["fields"]), FIELDS)
        for field in action["fields"].values():
            self.assertTrue(field["name"])
            self.assertTrue(field["description"])
            self.assertTrue(selector.validate_selector(field["selector"]))
        expiry = action["fields"]["ttl_seconds"]
        self.assertEqual(expiry["default"], 300)
        self.assertEqual(expiry["selector"]["number"]["min"], 60)
        self.assertEqual(expiry["selector"]["number"]["max"], 3600)
        self.assertEqual(expiry["selector"]["number"]["step"], 1)
        self.assertIn("512", action["fields"]["message"]["description"])
        self.assertFalse(action["fields"]["synthetic"]["default"])
        self.assertIn(
            "does not anonymize", action["fields"]["synthetic"]["description"]
        )
        self.assertIn("identical", action["fields"]["event_id"]["description"])
        self.assertIn("GitHub accepted", action["description"])
        self.assertIn("does not confirm consumer", action["description"])
        self.assertIn("durably queued", action["description"])
        self.assertIn("cancellation", action["description"])
        self.assertIn("sensitive", action["description"])
        self.assertNotIn("token", action["fields"])
        self.assertNotIn("repository", action["fields"])

    async def test_english_translations_load_for_config_options_and_action(self):
        source = json.loads((INTEGRATION / "strings.json").read_text())
        english = json.loads((INTEGRATION / "translations" / "en.json").read_text())
        self.assertEqual(source, english)
        loaded = {}
        for category in ("config", "options", "services", "selector"):
            loaded.update(
                await translation.async_get_translations(
                    self.hass, "en", category, integrations={DOMAIN}
                )
            )
        root = f"component.{DOMAIN}."
        for step in ("user", "reauth_confirm"):
            self.assertTrue(loaded[f"{root}config.step.{step}.title"])
            self.assertTrue(loaded[f"{root}config.step.{step}.description"])
            for field in source["config"]["step"][step]["data"]:
                self.assertTrue(loaded[f"{root}config.step.{step}.data.{field}"])
                self.assertTrue(
                    loaded[f"{root}config.step.{step}.data_description.{field}"]
                )
        for error in (
            "invalid_auth",
            "cannot_connect",
            "identity_changed",
            "invalid_pull_request_number",
            "rate_limited",
        ):
            self.assertTrue(loaded[f"{root}config.error.{error}"])
        for abort in ("already_configured", "reauth_successful"):
            self.assertTrue(loaded[f"{root}config.abort.{abort}"])
        self.assertEqual(
            loaded[f"{root}options.step.init.data.{CONF_ENABLED}"], "Enable sending"
        )
        self.assertIn(
            "initially disabled", loaded[f"{root}options.step.init.description"]
        )
        self.assertEqual(
            loaded[f"{root}services.send_event.name"], "Send notification event"
        )
        for field in FIELDS:
            self.assertTrue(loaded[f"{root}services.send_event.fields.{field}.name"])
            self.assertTrue(
                loaded[f"{root}services.send_event.fields.{field}.description"]
            )
        for severity in ("info", "warning", "urgent"):
            self.assertTrue(loaded[f"{root}selector.severity.options.{severity}"])


if __name__ == "__main__":
    unittest.main()
