#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN_DIR="$ROOT_DIR/bin"
STATE_DIR="$ROOT_DIR/.srdt-test-server"
UPSTREAM_BIN="$BIN_DIR/srdt-upstream-echo-linux-amd64"
SERVER_BIN="$BIN_DIR/srdt-server-linux-amd64"

usage()
{
  cat <<'USAGE'
Usage:
  ./scripts/srdt-test-server.sh start [--advertise IP_OR_HOST] [--port PORT]
  ./scripts/srdt-test-server.sh status
  ./scripts/srdt-test-server.sh stop
  ./scripts/srdt-test-server.sh logs

start launches:
  - local TCP echo upstream on 127.0.0.1:18443
  - SRDT UDP server on 0.0.0.0:19443 (or --port)

It prints one SRDT_TEST_TOKEN. Copy that token to the Windows test script.
Use --advertise if automatic address detection is not the address reachable from Windows.
USAGE
}

require_file()
{
  if [[ ! -x "$1" ]]; then
    echo "ERROR: executable not found: $1" >&2
    echo "Extract the full Stage 25 archive and run this script from that tree." >&2
    exit 2
  fi
}

pid_alive()
{
  local pid="$1"
  [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null
}

read_pid()
{
  local file="$1"
  if [[ -f "$file" ]]; then
    tr -d '[:space:]' < "$file"
  fi
}

auto_address()
{
  local address=""

  if command -v ip >/dev/null 2>&1; then
    address="$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{for (i=1; i<=NF; ++i) if ($i == "src" && (i+1) <= NF) {print $(i+1); exit}}')"
  fi

  if [[ -z "$address" ]] && command -v hostname >/dev/null 2>&1; then
    address="$(hostname -I 2>/dev/null | awk '{for (i=1; i<=NF; ++i) if ($i !~ /^127\./ && $i !~ /:/) {print $i; exit}}')"
  fi

  if [[ -z "$address" ]]; then
    address="127.0.0.1"
  fi

  printf '%s' "$address"
}

base64url()
{
  base64 | tr -d '\n=' | tr '+/' '-_'
}

stop_server()
{
  local server_pid upstream_pid
  server_pid="$(read_pid "$STATE_DIR/server.pid")"
  upstream_pid="$(read_pid "$STATE_DIR/upstream.pid")"

  if [[ -n "$server_pid" ]] && pid_alive "$server_pid"; then
    kill "$server_pid" 2>/dev/null || true
  fi
  if [[ -n "$upstream_pid" ]] && pid_alive "$upstream_pid"; then
    kill "$upstream_pid" 2>/dev/null || true
  fi

  for _ in $(seq 1 50); do
    local any=0
    if [[ -n "$server_pid" ]] && pid_alive "$server_pid"; then any=1; fi
    if [[ -n "$upstream_pid" ]] && pid_alive "$upstream_pid"; then any=1; fi
    [[ "$any" -eq 0 ]] && break
    sleep 0.1
  done

  if [[ -n "$server_pid" ]] && pid_alive "$server_pid"; then kill -9 "$server_pid" 2>/dev/null || true; fi
  if [[ -n "$upstream_pid" ]] && pid_alive "$upstream_pid"; then kill -9 "$upstream_pid" 2>/dev/null || true; fi

  rm -f "$STATE_DIR/server.pid" "$STATE_DIR/upstream.pid"
  echo "SRDT test server stopped."
}

start_server()
{
  local advertise=""
  local port="19443"
  local upstream_port="18443"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --advertise)
        [[ $# -ge 2 ]] || { echo "ERROR: --advertise needs a value" >&2; exit 2; }
        advertise="$2"
        shift 2
        ;;
      --port)
        [[ $# -ge 2 ]] || { echo "ERROR: --port needs a value" >&2; exit 2; }
        port="$2"
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "ERROR: unknown argument: $1" >&2
        usage >&2
        exit 2
        ;;
    esac
  done

  require_file "$UPSTREAM_BIN"
  require_file "$SERVER_BIN"

  mkdir -p "$STATE_DIR"

  local old_server old_upstream
  old_server="$(read_pid "$STATE_DIR/server.pid")"
  old_upstream="$(read_pid "$STATE_DIR/upstream.pid")"
  if { [[ -n "$old_server" ]] && pid_alive "$old_server"; } || { [[ -n "$old_upstream" ]] && pid_alive "$old_upstream"; }; then
    echo "ERROR: SRDT test server is already running."
    echo "Run: ./scripts/srdt-test-server.sh status"
    echo "or : ./scripts/srdt-test-server.sh stop"
    exit 1
  fi

  rm -f "$STATE_DIR"/*.log "$STATE_DIR"/*.json "$STATE_DIR"/*.token "$STATE_DIR"/*.env 2>/dev/null || true

  if [[ -z "$advertise" ]]; then
    advertise="$(auto_address)"
  fi

  if [[ "$advertise" == *[[:space:]\"]* ]]; then
    echo "ERROR: --advertise must be a plain IP address or hostname without spaces/quotes" >&2
    exit 2
  fi

  if [[ ! "$port" =~ ^[0-9]+$ ]] || (( port < 1 || port > 65535 )); then
    echo "ERROR: invalid UDP port: $port" >&2
    exit 2
  fi

  local psk
  psk="$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')"
  if [[ ${#psk} -ne 64 ]]; then
    echo "ERROR: failed to generate 32-byte PSK" >&2
    exit 1
  fi

  nohup "$UPSTREAM_BIN" \
    --listen "127.0.0.1:${upstream_port}" \
    >"$STATE_DIR/upstream.log" 2>&1 < /dev/null &
  local upstream_pid=$!
  printf '%s\n' "$upstream_pid" > "$STATE_DIR/upstream.pid"

  local upstream_ready=0
  for _ in $(seq 1 100); do
    if ! pid_alive "$upstream_pid"; then
      echo "ERROR: upstream process exited during startup" >&2
      cat "$STATE_DIR/upstream.log" >&2 || true
      rm -f "$STATE_DIR/upstream.pid"
      exit 1
    fi
    if grep -q '^LISTEN_ADDR=' "$STATE_DIR/upstream.log" 2>/dev/null; then
      upstream_ready=1
      break
    fi
    sleep 0.05
  done
  if [[ "$upstream_ready" -ne 1 ]]; then
    echo "ERROR: upstream did not become ready" >&2
    stop_server >/dev/null || true
    cat "$STATE_DIR/upstream.log" >&2 || true
    exit 1
  fi

  nohup env GOMAXPROCS=2 GOMEMLIMIT=128MiB "$SERVER_BIN" \
    --multi \
    --listen "0.0.0.0:${port}" \
    --psk "$psk" \
    --upstream "127.0.0.1:${upstream_port}" \
    --idle-timeout 2s \
    --max-sessions 1200 \
    --max-per-ip 1200 \
    --metrics-file "$STATE_DIR/server-metrics.json" \
    >"$STATE_DIR/server.log" 2>&1 < /dev/null &
  local server_pid=$!
  printf '%s\n' "$server_pid" > "$STATE_DIR/server.pid"

  local public_key=""
  for _ in $(seq 1 200); do
    if ! pid_alive "$server_pid"; then
      echo "ERROR: SRDT server exited during startup" >&2
      cat "$STATE_DIR/server.log" >&2 || true
      stop_server >/dev/null || true
      exit 1
    fi
    public_key="$(sed -n 's/^SERVER_PUBLIC=//p' "$STATE_DIR/server.log" | head -n 1)"
    [[ -n "$public_key" ]] && break
    sleep 0.05
  done

  if [[ ! "$public_key" =~ ^[0-9a-fA-F]{64}$ ]]; then
    echo "ERROR: failed to obtain SERVER_PUBLIC" >&2
    cat "$STATE_DIR/server.log" >&2 || true
    stop_server >/dev/null || true
    exit 1
  fi

  local payload token
  payload="$(printf '{\"v\":1,\"host\":\"%s\",\"port\":%s,\"psk\":\"%s\",\"pub\":\"%s\"}' "$advertise" "$port" "$psk" "$public_key")"
  token="SRDT1.$(printf '%s' "$payload" | base64url)"

  cat > "$STATE_DIR/config.env" <<CONFIG
ADVERTISE=$advertise
PORT=$port
PSK=$psk
SERVER_PUBLIC=$public_key
UPSTREAM_PORT=$upstream_port
CONFIG
  printf '%s\n' "$token" > "$STATE_DIR/connection.token"

  cat <<RESULT

============================================================
SRDT TEST SERVER STARTED
============================================================
Advertised address : $advertise
UDP port           : $port
Server PID         : $server_pid
Upstream PID       : $upstream_pid

SRDT_TEST_TOKEN=$token

On Windows, from the extracted Stage 25 directory, run exactly:

  .\\scripts\\Test-SRDT.cmd -Token '$token'

Optional stress test:

  .\\scripts\\Test-SRDT.cmd -Token '$token' -StressClients 1000

To stop this Linux test server:

  ./scripts/srdt-test-server.sh stop

If Windows cannot reach the server, make sure UDP/$port is allowed by the host/cloud firewall.
If '$advertise' is not the address reachable from Windows, restart with:

  ./scripts/srdt-test-server.sh stop
  ./scripts/srdt-test-server.sh start --advertise YOUR_REACHABLE_IP
============================================================
RESULT
}

show_status()
{
  local server_pid upstream_pid
  server_pid="$(read_pid "$STATE_DIR/server.pid")"
  upstream_pid="$(read_pid "$STATE_DIR/upstream.pid")"

  echo "SRDT test server status:"
  if [[ -n "$server_pid" ]] && pid_alive "$server_pid"; then
    echo "  server   : RUNNING pid=$server_pid"
  else
    echo "  server   : STOPPED"
  fi
  if [[ -n "$upstream_pid" ]] && pid_alive "$upstream_pid"; then
    echo "  upstream : RUNNING pid=$upstream_pid"
  else
    echo "  upstream : STOPPED"
  fi

  if [[ -f "$STATE_DIR/config.env" ]]; then
    # shellcheck disable=SC1090
    source "$STATE_DIR/config.env"
    echo "  address  : ${ADVERTISE}:${PORT}/udp"
    echo "  public   : ${SERVER_PUBLIC}"
  fi
  if [[ -f "$STATE_DIR/connection.token" ]]; then
    echo "  token    : $(cat "$STATE_DIR/connection.token")"
  fi
}

show_logs()
{
  echo "===== upstream.log ====="
  cat "$STATE_DIR/upstream.log" 2>/dev/null || true
  echo
  echo "===== server.log ====="
  cat "$STATE_DIR/server.log" 2>/dev/null || true
  if [[ -f "$STATE_DIR/server-metrics.json" ]]; then
    echo
    echo "===== server-metrics.json ====="
    cat "$STATE_DIR/server-metrics.json"
  fi
}

command="${1:-}"
case "$command" in
  start)
    shift
    start_server "$@"
    ;;
  stop)
    stop_server
    ;;
  status)
    show_status
    ;;
  logs)
    show_logs
    ;;
  -h|--help|help|"")
    usage
    ;;
  *)
    echo "ERROR: unknown command: $command" >&2
    usage >&2
    exit 2
    ;;
esac
