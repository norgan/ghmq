"""Constants for the outbound-only GHMQ bridge."""

DOMAIN = "ghmq"
VERSION = "0.1.1"
CONF_OWNER = "owner"
CONF_REPOSITORY = "repository"
CONF_BRANCH = "branch"
CONF_PR_NUMBER = "pull_request_number"
CONF_TOKEN = "token"
CONF_ENABLED = "sending_enabled"
API_ROOT = "https://api.github.com"
MAX_PAYLOAD_BYTES = 4096
MAX_PENDING = 100
MAX_RECENT = 1000
DEFAULT_TTL = 300

MAX_JOURNAL_BYTES = 4 * 1024 * 1024
JOURNAL_VERSION = 2

CONF_REPOSITORY_ID = "repository_id"
CONF_PULL_REQUEST_ID = "pull_request_id"
