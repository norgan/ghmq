"""Deliberately allow-listed diagnostics, excluding all configuration and events."""

from .const import VERSION


async def async_get_config_entry_diagnostics(hass, entry):
    result = {
        "version": VERSION,
        "loaded": getattr(entry, "runtime_data", None) is not None,
    }
    if result["loaded"]:
        result["sending_enabled"] = entry.runtime_data.enabled
        result.update(entry.runtime_data.sender.diagnostics())
    return result
