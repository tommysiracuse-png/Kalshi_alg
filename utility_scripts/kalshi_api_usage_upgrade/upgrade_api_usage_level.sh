#!/usr/bin/env bash
set -euo pipefail

readonly REQUEST_METHOD="POST"
readonly REQUEST_PATH="/trade-api/v2/account/api_usage_level/upgrade"

base_url="${KALSHI_API_BASE_URL:-https://external-api.kalshi.com}"
assume_yes=false

usage() {
  printf '%s\n' \
    "Usage: $(basename "$0") [--yes] [--demo | --base-url URL]" \
    "" \
    "Requests Kalshi's permanent Advanced API usage-level grant." \
    "" \
    "Required environment variables:" \
    "  KALSHI_API_KEY_ID          Kalshi API key ID" \
    "  KALSHI_PRIVATE_KEY_PATH    Path to the corresponding RSA private key" \
    "" \
    "Options:" \
    "  --yes             Skip the interactive confirmation" \
    "  --demo            Use https://external-api.demo.kalshi.co" \
    "  --base-url URL    Override the API host" \
    "  -h, --help        Show this help"
}

while (($# > 0)); do
  case "$1" in
    --yes)
      assume_yes=true
      shift
      ;;
    --demo)
      base_url="https://external-api.demo.kalshi.co"
      shift
      ;;
    --base-url)
      if (($# < 2)); then
        printf 'ERROR: --base-url requires a value\n' >&2
        exit 2
      fi
      base_url="${2%/}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'ERROR: unknown argument: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

for command_name in curl openssl date mktemp; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    printf 'ERROR: required command not found: %s\n' "$command_name" >&2
    exit 2
  fi
done

api_key_id="${KALSHI_API_KEY_ID:-}"
private_key_path="${KALSHI_PRIVATE_KEY_PATH:-}"

if [[ -z "$api_key_id" ]]; then
  printf 'ERROR: KALSHI_API_KEY_ID is not set\n' >&2
  exit 2
fi

if [[ -z "$private_key_path" ]]; then
  printf 'ERROR: KALSHI_PRIVATE_KEY_PATH is not set\n' >&2
  exit 2
fi

if [[ ! -f "$private_key_path" ]]; then
  printf 'ERROR: private key file does not exist: %s\n' "$private_key_path" >&2
  exit 2
fi

if ! openssl pkey -in "$private_key_path" -noout >/dev/null 2>&1; then
  printf 'ERROR: unable to load RSA private key: %s\n' "$private_key_path" >&2
  exit 2
fi

if [[ "$assume_yes" != true ]]; then
  if [[ ! -t 0 ]]; then
    printf 'ERROR: interactive confirmation is unavailable; pass --yes to execute\n' >&2
    exit 2
  fi
  printf '%s\n' \
    "This will request a permanent Advanced API usage-level grant." \
    "Endpoint cost: 30 Predictions Write tokens." \
    "Target: ${base_url}${REQUEST_PATH}"
  read -r -p "Continue? [y/N] " confirmation
  if [[ ! "$confirmation" =~ ^[Yy]$ ]]; then
    printf 'Canceled.\n'
    exit 0
  fi
fi

timestamp_ns="$(date +%s%N)"
if [[ ! "$timestamp_ns" =~ ^[0-9]+$ ]]; then
  printf 'ERROR: date did not return a numeric Unix timestamp\n' >&2
  exit 2
fi
timestamp_ms="$((timestamp_ns / 1000000))"
signed_message="${timestamp_ms}${REQUEST_METHOD}${REQUEST_PATH}"
signature="$({
  printf '%s' "$signed_message" |
    openssl dgst -sha256 \
      -sign "$private_key_path" \
      -sigopt rsa_padding_mode:pss \
      -sigopt rsa_pss_saltlen:32
} | openssl base64 -A)"

response_file="$(mktemp)"
trap 'rm -f "$response_file"' EXIT

http_status="$(curl \
  --silent \
  --show-error \
  --request "$REQUEST_METHOD" \
  --header "KALSHI-ACCESS-KEY: ${api_key_id}" \
  --header "KALSHI-ACCESS-TIMESTAMP: ${timestamp_ms}" \
  --header "KALSHI-ACCESS-SIGNATURE: ${signature}" \
  --header "Accept: application/json" \
  --header "Content-Type: application/json" \
  --data '{}' \
  --output "$response_file" \
  --write-out '%{http_code}' \
  "${base_url}${REQUEST_PATH}")"

if [[ -s "$response_file" ]]; then
  if command -v jq >/dev/null 2>&1 && jq empty "$response_file" >/dev/null 2>&1; then
    jq . "$response_file"
  else
    sed -n '1,$p' "$response_file"
  fi
fi

if [[ "$http_status" == "201" ]]; then
  printf 'Kalshi API usage-level upgrade succeeded (HTTP %s).\n' "$http_status"
  exit 0
fi

printf 'Kalshi API usage-level upgrade failed (HTTP %s).\n' "$http_status" >&2
exit 1
