#!/usr/bin/env bash
set -Eeuo pipefail

# Start and verify the Proton WireGuard -> Tinyproxy path used by Polymarket.
# Run this script as root:
#   sudo tools/start_polymarket_proxy.sh

NAMESPACE="${POLYMARKET_NETNS:-pm-vpn}"
HOST_IFACE="${POLYMARKET_NETNS_HOST_IFACE:-pm-vpn-host}"
PEER_IFACE="${POLYMARKET_NETNS_PEER_IFACE:-pm-vpn-peer}"
HOST_ADDR="${POLYMARKET_NETNS_HOST_ADDR:-10.200.0.1/24}"
PROXY_ADDR="${POLYMARKET_PROXY_ADDR:-10.200.0.2}"
PROXY_PORT="${POLYMARKET_PROXY_PORT:-18080}"
WG_CONFIG="${POLYMARKET_WG_CONFIG:-$HOME/.config/proton/polymarket.conf}"
WG_IFACE="${POLYMARKET_WG_IFACE:-polymarket}"
WG_MTU="${POLYMARKET_WG_MTU:-1280}"
FORCE_WG_RESTART="${POLYMARKET_FORCE_WG_RESTART:-0}"
TINYPROXY_CONFIG="${POLYMARKET_TINYPROXY_CONFIG:-/etc/tinyproxy/pm-vpn.conf}"
LOG_FILE="${POLYMARKET_TINYPROXY_LOG:-/var/log/tinyproxy-pm-vpn.log}"
PID_FILE="${POLYMARKET_TINYPROXY_PID:-/run/tinyproxy-pm-vpn.pid}"
TEST_URL="${POLYMARKET_PROXY_TEST_URL:-https://gamma-api.polymarket.com/markets?limit=1}"

die() {
    echo "ERROR: $*" >&2
    exit 1
}

info() {
    echo "==> $*"
}

command -v ip >/dev/null || die "ip is required"
command -v wg >/dev/null || die "wireguard-tools (wg) is required"
command -v wg-quick >/dev/null || die "wg-quick is required"
command -v tinyproxy >/dev/null || die "tinyproxy is required"
command -v curl >/dev/null || die "curl is required"
command -v ss >/dev/null || die "iproute2 (ss) is required"
command -v iptables >/dev/null || die "iptables is required"

[[ "$(id -u)" == "0" ]] || die "run as root: sudo $0"
[[ "$WG_MTU" =~ ^[0-9]+$ && "$WG_MTU" -ge 576 && "$WG_MTU" -le 1800 ]] || \
    die "POLYMARKET_WG_MTU must be an integer between 576 and 1800"
# sudo may change HOME to /root. Resolve the invoking user's home directory so
# the default still points at that user's Proton profile.
if [[ -n "${SUDO_USER:-}" ]]; then
    INVOKING_HOME="$(getent passwd "$SUDO_USER" | cut -d: -f6)"
    [[ -n "$INVOKING_HOME" ]] && WG_CONFIG="${POLYMARKET_WG_CONFIG:-$INVOKING_HOME/.config/proton/polymarket.conf}"
fi
[[ -r "$WG_CONFIG" ]] || die "WireGuard config not found: $WG_CONFIG"
[[ -r "$TINYPROXY_CONFIG" ]] || die "Tinyproxy config not found: $TINYPROXY_CONFIG"
grep -Eq "^[[:space:]]*Listen[[:space:]]+${PROXY_ADDR}([[:space:]]|$)" "$TINYPROXY_CONFIG" || \
    die "$TINYPROXY_CONFIG does not listen on $PROXY_ADDR"
grep -Eq "^[[:space:]]*Port[[:space:]]+${PROXY_PORT}([[:space:]]|$)" "$TINYPROXY_CONFIG" || \
    die "$TINYPROXY_CONFIG does not use port $PROXY_PORT"

if [[ "$(stat -c '%a' "$WG_CONFIG" 2>/dev/null || echo 0)" != "600" ]]; then
    echo "WARNING: $WG_CONFIG is not mode 600; it contains a private key." >&2
fi

UPLINK="${POLYMARKET_UPLINK:-$(ip -o route show default | awk 'NR == 1 {print $5}') }"
UPLINK="${UPLINK//[[:space:]]/}"
[[ -n "$UPLINK" ]] || die "could not determine the host default-route interface; set POLYMARKET_UPLINK"

info "Using uplink $UPLINK"

if ! ip netns list | awk '{print $1}' | grep -Fxq "$NAMESPACE"; then
    info "Creating network namespace $NAMESPACE"
    mkdir -p /run/netns
    ip netns add "$NAMESPACE"
fi

if ! ip link show "$HOST_IFACE" >/dev/null 2>&1; then
    info "Creating veth pair $HOST_IFACE/$PEER_IFACE"
    ip link add "$HOST_IFACE" type veth peer name "$PEER_IFACE"
    ip link set "$PEER_IFACE" netns "$NAMESPACE"
elif ! ip netns exec "$NAMESPACE" ip link show "$PEER_IFACE" >/dev/null 2>&1; then
    die "$HOST_IFACE exists but $PEER_IFACE is not in namespace $NAMESPACE"
fi

ip addr replace "$HOST_ADDR" dev "$HOST_IFACE"
ip link set "$HOST_IFACE" up
ip netns exec "$NAMESPACE" ip link set lo up
ip netns exec "$NAMESPACE" ip addr replace "$PROXY_ADDR/24" dev "$PEER_IFACE"
ip netns exec "$NAMESPACE" ip link set "$PEER_IFACE" up
ip netns exec "$NAMESPACE" ip route replace default via "${HOST_ADDR%/*}" dev "$PEER_IFACE"

sysctl -w net.ipv4.ip_forward=1 >/dev/null

if ! iptables -C FORWARD -i "$HOST_IFACE" -o "$UPLINK" -j ACCEPT 2>/dev/null; then
    iptables -A FORWARD -i "$HOST_IFACE" -o "$UPLINK" -j ACCEPT
fi
if ! iptables -C FORWARD -i "$UPLINK" -o "$HOST_IFACE" \
    -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT 2>/dev/null; then
    iptables -A FORWARD -i "$UPLINK" -o "$HOST_IFACE" \
        -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
fi
if ! iptables -t nat -C POSTROUTING -s "${HOST_ADDR%/*}"/24 -o "$UPLINK" -j MASQUERADE 2>/dev/null; then
    iptables -t nat -A POSTROUTING -s "${HOST_ADDR%/*}"/24 -o "$UPLINK" -j MASQUERADE
fi

if ! ip netns exec "$NAMESPACE" wg show "$WG_IFACE" >/dev/null 2>&1; then
    info "Starting WireGuard interface $WG_IFACE"
    ip netns exec "$NAMESPACE" wg-quick up "$WG_CONFIG"
else
    # An interface can survive a failed previous run without ever completing
    # a handshake. Recreate that stale state before starting Tinyproxy.
    LAST_HANDSHAKE="$(ip netns exec "$NAMESPACE" wg show "$WG_IFACE" latest-handshakes 2>/dev/null | awk '$2 > 0 {print $2; exit}')"
    if [[ "$FORCE_WG_RESTART" == "1" || -z "$LAST_HANDSHAKE" ]]; then
        info "Restarting WireGuard interface $WG_IFACE"
        ip netns exec "$NAMESPACE" wg-quick down "$WG_CONFIG" >/dev/null 2>&1 || true
        ip netns exec "$NAMESPACE" wg-quick up "$WG_CONFIG"
    else
        info "WireGuard interface $WG_IFACE is already up"
    fi
fi

# A lower MTU avoids black-holing TLS packets on VPN paths with a smaller
# effective PMTU. Override with POLYMARKET_WG_MTU=1420 if the path supports it.
ip netns exec "$NAMESPACE" ip link set dev "$WG_IFACE" mtu "$WG_MTU"
info "WireGuard MTU set to $WG_MTU"

info "Checking direct egress from namespace"
DIRECT_STATUS="$(ip netns exec "$NAMESPACE" curl -4 -sS --max-time 10 --noproxy '' \
    -o /dev/null -w '%{http_code}' "$TEST_URL" 2>/dev/null || true)"
if [[ ! "$DIRECT_STATUS" =~ ^[0-9]{3}$ || "$DIRECT_STATUS" == "000" ]]; then
    echo "ERROR: WireGuard namespace cannot reach $TEST_URL" >&2
    echo "Namespace routes:" >&2
    ip netns exec "$NAMESPACE" ip route >&2 || true
    echo "Namespace policy routes:" >&2
    ip netns exec "$NAMESPACE" ip -4 rule >&2 || true
    ip netns exec "$NAMESPACE" ip -4 route show table 51820 >&2 || true
    echo "Namespace resolver:" >&2
    ip netns exec "$NAMESPACE" cat /etc/resolv.conf >&2 || true
    echo "WireGuard status:" >&2
    ip netns exec "$NAMESPACE" wg show "$WG_IFACE" >&2 || true
    exit 1
fi
info "Direct namespace egress returned HTTP $DIRECT_STATUS"

if ! ip netns exec "$NAMESPACE" ss -ltn 2>/dev/null | awk '{print $4}' | grep -Eq "(^|:)${PROXY_PORT}$"; then
    info "Starting Tinyproxy on ${PROXY_ADDR}:${PROXY_PORT}"
    mkdir -p "$(dirname "$LOG_FILE")"
    ip netns exec "$NAMESPACE" tinyproxy -d -c "$TINYPROXY_CONFIG" >>"$LOG_FILE" 2>&1 &
    echo "$!" >"$PID_FILE"
    sleep 1
    if ! ip netns exec "$NAMESPACE" ss -ltn 2>/dev/null | awk '{print $4}' | grep -Eq "(^|:)${PROXY_PORT}$"; then
        echo "ERROR: Tinyproxy did not bind ${PROXY_ADDR}:${PROXY_PORT}" >&2
        tail -40 "$LOG_FILE" >&2 || true
        exit 1
    fi
else
    info "Tinyproxy is already listening on ${PROXY_ADDR}:${PROXY_PORT}"
fi

for _ in $(seq 1 12); do
    PROXY_STATUS="$(curl -sS --max-time 5 --noproxy '' \
        --proxy "http://${PROXY_ADDR}:${PROXY_PORT}" \
        -o /dev/null -w '%{http_code}' "$TEST_URL" 2>/dev/null || true)"
    if [[ "$PROXY_STATUS" =~ ^[0-9]{3}$ && "$PROXY_STATUS" != "000" ]]; then
        echo
        echo "Proxy is working: http://${PROXY_ADDR}:${PROXY_PORT}"
        echo "Proxy request returned HTTP $PROXY_STATUS"
        echo "Set POLYMARKET_PROXY_URL=http://${PROXY_ADDR}:${PROXY_PORT} for the launcher."
        echo
        ip netns exec "$NAMESPACE" wg show "$WG_IFACE" | sed -E 's/(private key:).*/\1 <redacted>/I'
        exit 0
    fi
    printf '.' >&2
    sleep 1
done
echo >&2

echo "ERROR: proxy did not complete a request through $TEST_URL" >&2
echo "Namespace routes:" >&2
ip netns exec "$NAMESPACE" ip route >&2 || true
echo "Namespace policy routes:" >&2
ip netns exec "$NAMESPACE" ip -4 rule >&2 || true
ip netns exec "$NAMESPACE" ip -4 route show table 51820 >&2 || true
echo "Namespace resolver:" >&2
ip netns exec "$NAMESPACE" cat /etc/resolv.conf >&2 || true
echo "WireGuard status:" >&2
ip netns exec "$NAMESPACE" wg show "$WG_IFACE" >&2 || true
echo "Tinyproxy log:" >&2
tail -40 "$LOG_FILE" >&2 || true
exit 1
