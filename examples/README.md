# Synthetic examples

These examples contain invented data only. They still create real files in the configured private relay once GHMQ sending is enabled. A receiver must explicitly decide how to handle `synthetic: true`; the sender does not suppress receiver effects.

- `synthetic-action.yaml`: one manual action, or one action within a script/automation
- `synthetic-automation.yaml`: one automation triggered only by an explicit local `ghmq_demo_requested` event
- `event-v1.json`: canonical wire-format example with nine required fields and no optional entity ID

Select `config_entry_id` through Home Assistant when more than one GHMQ integration is loaded. The action and automation omit `event_id`, so each call creates a new ID. For retries of the same occurrence, provide the same stable ID and identical content as described in the [README](../README.md#retry-the-same-occurrence-safely).

The JSON file has fixed demonstration timestamps and is not a live test payload. It must not be replayed as fresh. Use the action to generate a current event, then check GitHub and the separate receiver independently. `accepted` confirms sender-side GitHub acceptance only.
