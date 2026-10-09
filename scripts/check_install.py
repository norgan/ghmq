"""Cold-process HA discovery/config-flow check: python -I scripts/check_install.py ZIP.

Only inert local credentials and mocked GitHub HTTP responses are used.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import sys
import tempfile
import zipfile
from pathlib import Path, PurePosixPath
from unittest.mock import patch

from homeassistant import config_entries, loader, setup
from homeassistant.core import HomeAssistant


class Response:
    def __init__(self, body):
        self.status = 200
        self.headers = {}
        self.content = self
        self.raw = json.dumps(body).encode()

    async def read(self, size):
        chunk, self.raw = self.raw[:size], self.raw[size:]
        return chunk

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


class Session:
    def __init__(self):
        self.methods = []

    def request(self, method, url, **_kwargs):
        assert method == "GET", "Cold setup must never write an event"
        self.methods.append(method)
        if url == "https://api.github.com/repos/example-owner/private-events":
            return Response({"id": 101, "private": True, "default_branch": "main"})
        assert (
            url == "https://api.github.com/repos/example-owner/private-events/pulls/1"
        )
        return Response(
            {
                "id": 202,
                "state": "open",
                "head": {"ref": "notification-events", "repo": {"id": 101}},
            }
        )


async def check(path: Path) -> None:
    assert "custom_components" not in sys.modules, "Run in a fresh process"
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
            assert len(names) == len(set(names)), "Duplicate archive paths"
            for name in names:
                member = PurePosixPath(name)
                assert not member.is_absolute() and ".." not in member.parts
                assert member.parts[:2] == ("custom_components", "ghmq")
                assert not name.endswith(".pyc") and "__pycache__" not in member.parts
            archive.extractall(root)
        hass = HomeAssistant(str(root))
        hass.config.skip_pip = True
        hass.config_entries = config_entries.ConfigEntries(hass, {})
        loader.async_setup(hass)
        try:
            with patch(
                "aiohttp.ClientSession._request",
                side_effect=AssertionError("External HTTP is forbidden"),
            ):
                discovered = await loader.async_get_custom_components(hass)
                assert "ghmq" in discovered
                assert "ghmq" in await loader.async_get_config_flows(hass)
                adapter = importlib.import_module("custom_components.ghmq")
                session = Session()
                with patch.object(
                    adapter, "async_get_clientsession", return_value=session
                ):
                    assert await setup.async_setup_component(hass, "ghmq", {})
                    form = await hass.config_entries.flow.async_init(
                        "ghmq", context={"source": "user"}
                    )
                    assert form["type"] == "form" and form["step_id"] == "user"
                    result = await hass.config_entries.flow.async_configure(
                        form["flow_id"],
                        {
                            "owner": "example-owner",
                            "repository": "private-events",
                            "branch": "notification-events",
                            "pull_request_number": 1,
                            "token": "inert-cold-process-placeholder",
                        },
                    )
                    assert result["type"] == "create_entry"
                    await hass.async_block_till_done()
                entry = result["result"]
                assert entry.data["repository_id"] == 101
                assert entry.data["pull_request_id"] == 202
                assert entry.options["sending_enabled"] is False
                assert entry.state is config_entries.ConfigEntryState.LOADED
                assert hass.services.has_service("ghmq", "send_event")
                assert len(session.methods) == 4
                await hass.config_entries.async_unload(entry.entry_id)
                assert not hasattr(entry, "runtime_data")
                print(
                    json.dumps(
                        {
                            "discovered": True,
                            "config_flow": "create_entry",
                            "setup": "loaded",
                            "sending_enabled": False,
                            "mock_http_gets": len(session.methods),
                            "puts": 0,
                            "unload": "passed",
                        }
                    )
                )
        finally:
            await hass.async_stop(force=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    asyncio.run(check(parser.parse_args().archive))


if __name__ == "__main__":
    main()
