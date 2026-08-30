# Kalshi API usage-level upgrade

This directory contains a standalone Bash client for Kalshi's
`POST /trade-api/v2/account/api_usage_level/upgrade` endpoint.

The endpoint permanently grants or refreshes the Advanced API usage level. It
costs 30 Predictions Write tokens. Kalshi requires at least one of the user's
latest 100 Predictions orders to have been created through the API.

## Run

From the repository root:

```bash
export KALSHI_API_KEY_ID="your-api-key-id"
export KALSHI_PRIVATE_KEY_PATH="/absolute/path/to/private-key.pem"
./kalshi_api_usage_upgrade/upgrade_api_usage_level.sh
```

The script asks for confirmation before sending the request. For unattended
execution, pass `--yes`:

```bash
./kalshi_api_usage_upgrade/upgrade_api_usage_level.sh --yes
```

Although the endpoint has no request fields, the script sends an empty JSON
object because Kalshi requires authenticated POST requests to use the
`application/json` content type.

Production is the default. Use `--demo` for Kalshi's demo host or
`--base-url URL` for an explicit host. The script never prints the private key
or API key ID.

Official documentation:
<https://docs.kalshi.com/api-reference/account/upgrade-account-api-usage-level>
