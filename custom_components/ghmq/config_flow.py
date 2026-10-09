"""UI configuration; creating an entry performs reads, never a test commit."""

from __future__ import annotations

import logging
import math
from datetime import datetime, timezone

import probatio
from homeassistant import config_entries
from homeassistant.core import callback
from homeassistant.helpers import selector

from . import async_validate_config
from .const import (
    CONF_BRANCH,
    CONF_ENABLED,
    CONF_OWNER,
    CONF_PR_NUMBER,
    CONF_REPOSITORY,
    CONF_TOKEN,
    DOMAIN,
)
from .core import (
    AuthError,
    BridgeError,
    IdentityError,
    JournalError,
    RateLimitError,
    RetryableError,
)

_LOGGER = logging.getLogger(__name__)

# Only fixed, locally defined categories reach logs. Never format exceptions,
# user input, request URLs/headers, response bodies, or credentials.
_ERROR_CATEGORIES = {
    "Invalid GitHub owner": "invalid_owner",
    "Invalid GitHub repository": "invalid_repository",
    "Invalid relay branch": "invalid_branch",
    "Invalid pull request number": "invalid_pull_request_number",
    "Bridge requires an active private repository": "private_repository_required",
    "Relay branch must be separate from the default branch": "default_branch_rejected",
    "Relay requires an open PR from the configured branch in the same repository": "pull_request_mismatch",
    "GitHub response exceeds size limit": "response_too_large",
    "GitHub returned an invalid response": "invalid_response",
    "GitHub denied access; check the repository-scoped credential": "access_denied",
    "GitHub redirected the request; verify the configured destination": "redirect_rejected",
    "GitHub request rejected; check the configured private repository and PR": "request_rejected",
}


def log_validation_failure(error):
    if isinstance(error, AuthError):
        category = "authentication"
    elif isinstance(error, IdentityError):
        category = "identity"
    elif isinstance(error, JournalError):
        category = "journal"
    elif isinstance(error, RateLimitError):
        category = "rate_limited"
    elif isinstance(error, RetryableError):
        category = "retry_or_cooldown"
    else:
        message = error.args[0] if error.args else None
        category = (
            _ERROR_CATEGORIES.get(message, "validation")
            if type(message) is str
            else "validation"
        )
    _LOGGER.warning("GHMQ validation failed: category=%s", category)


def retry_deadline(error):
    try:
        return datetime.fromtimestamp(math.ceil(error.retry_at), timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
    except (OverflowError, OSError, TypeError, ValueError):
        # A finite server deadline can exceed datetime's supported year range.
        # Do not shorten that deadline or expose raw values/exceptions.
        return "a future GitHub reset time"


def valid_pr_number(value):
    return (
        type(value) is int
        or (type(value) is float and math.isfinite(value) and value.is_integer())
    ) and value >= 1


class PullRequestNumberSelector(selector.NumberSelector):
    """Keep the number UI while rejecting booleans/fractions before coercion."""

    def __call__(self, value):
        if not valid_pr_number(value):
            raise probatio.Invalid("Pull request number must be a positive integer")
        return int(value)


def token_selector():
    return selector.TextSelector({"type": selector.TextSelectorType.PASSWORD})


class ConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1

    async def async_step_user(self, user_input=None):
        errors = {}
        description_placeholders = {}
        if user_input is not None and not valid_pr_number(
            user_input.get(CONF_PR_NUMBER)
        ):
            errors[CONF_PR_NUMBER] = "invalid_pull_request_number"
            user_input = None
        if user_input is not None:
            # Accept an integral JSON float without truncating a fraction.
            # Persist the integer type required by the transport.
            user_input = {**user_input, CONF_PR_NUMBER: int(user_input[CONF_PR_NUMBER])}
            try:
                identity = await async_validate_config(
                    self.hass, user_input, discover=True
                )
            except AuthError as err:
                log_validation_failure(err)
                errors["base"] = "invalid_auth"
            except IdentityError as err:
                log_validation_failure(err)
                errors["base"] = "identity_changed"
            except RateLimitError as err:
                log_validation_failure(err)
                errors["base"] = "rate_limited"
                description_placeholders["retry_at"] = retry_deadline(err)
            except BridgeError as err:
                log_validation_failure(err)
                errors["base"] = "cannot_connect"
            else:
                # One journal/serializer per GitHub destination, not one per PR.
                await self.async_set_unique_id(
                    f"{user_input[CONF_OWNER].lower()}/{user_input[CONF_REPOSITORY].lower()}:{user_input[CONF_BRANCH]}"
                )
                self._abort_if_unique_id_configured()
                return self.async_create_entry(
                    title="GHMQ",
                    data={**user_input, **identity},
                    options={CONF_ENABLED: False},
                )
        return self.async_show_form(
            step_id="user",
            errors=errors,
            description_placeholders=description_placeholders,
            data_schema=probatio.Schema(
                {
                    probatio.Required(CONF_OWNER): str,
                    probatio.Required(CONF_REPOSITORY): str,
                    probatio.Required(CONF_BRANCH, default="relay/events"): str,
                    probatio.Required(CONF_PR_NUMBER): PullRequestNumberSelector(
                        {"min": 1, "step": 1, "mode": selector.NumberSelectorMode.BOX}
                    ),
                    probatio.Required(CONF_TOKEN): token_selector(),
                }
            ),
        )

    async def async_step_reauth(self, entry_data):
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(self, user_input=None):
        entry = self._get_reauth_entry()
        errors = {}
        description_placeholders = {}
        if user_input is not None:
            data = {**entry.data, CONF_TOKEN: user_input[CONF_TOKEN]}
            try:
                # Reauthentication changes only the token. The pinned server
                # identities must already exist and must still match GitHub.
                await async_validate_config(
                    self.hass, data, entry=entry, discover=False
                )
            except AuthError as err:
                log_validation_failure(err)
                errors["base"] = "invalid_auth"
            except IdentityError as err:
                log_validation_failure(err)
                errors["base"] = "identity_changed"
            except RateLimitError as err:
                log_validation_failure(err)
                errors["base"] = "rate_limited"
                description_placeholders["retry_at"] = retry_deadline(err)
            except BridgeError as err:
                log_validation_failure(err)
                errors["base"] = "cannot_connect"
            else:
                return self.async_update_reload_and_abort(
                    entry, data_updates={CONF_TOKEN: user_input[CONF_TOKEN]}
                )
        return self.async_show_form(
            step_id="reauth_confirm",
            errors=errors,
            description_placeholders=description_placeholders,
            data_schema=probatio.Schema(
                {probatio.Required(CONF_TOKEN): token_selector()}
            ),
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        return OptionsFlow()


class OptionsFlow(config_entries.OptionsFlowWithReload):
    async def async_step_init(self, user_input=None):
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)
        return self.async_show_form(
            step_id="init",
            data_schema=probatio.Schema(
                {
                    probatio.Required(
                        CONF_ENABLED,
                        default=self.config_entry.options.get(CONF_ENABLED, False),
                    ): bool,
                }
            ),
        )
