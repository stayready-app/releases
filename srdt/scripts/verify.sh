#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

run_group() {
  local name=$1
  local pattern=$2
  echo "=== netreliable: ${name} ==="
  go test -count=1 -timeout=120s ./netreliable -run "$pattern"
}

run_race_group() {
  local name=$1
  local pattern=$2
  echo "=== netreliable race: ${name} ==="
  go test -race -count=1 -timeout=180s ./netreliable -run "$pattern"
}

echo '=== toolchain ==='
go version
uname -a

echo '=== formatting ==='
files=$(gofmt -l cmd emulator netreliable reliable secureudp testkit)
if [[ -n "$files" ]]; then
  printf '%s\n' "$files"
  exit 1
fi

echo '=== build ==='
go build ./...

echo '=== vet ==='
go vet ./...

echo '=== core tests ==='
go test -count=1 ./reliable ./secureudp ./testkit ./emulator

run_group audit '^TestAudit'
run_group congestion-credits '^Test(Congestion|Credits)'
run_group transport '^Test(Batch|RealUDP|Packet|Multi)'
run_group stages '^TestStage(21|22|23|24)'
run_group window-wan '^Test(Stage16WAN|Window)'

echo '=== core race ==='
go test -race -count=1 ./reliable ./secureudp ./testkit ./emulator

run_race_group audit '^TestAudit'
run_race_group congestion-credits '^Test(Congestion|Credits)'
run_race_group transport '^Test(Batch|RealUDP|Packet|Multi)'
run_race_group stages '^TestStage(21|22|23|24)'
run_race_group window-wan '^Test(Stage16WAN|Window)'

echo '=== repeated Stage 23 regressions ==='
go test -count=3 -timeout=120s ./netreliable -run '^TestStage23'

echo '=== repeated Stage 24 regressions ==='
go test -count=3 -timeout=120s ./netreliable -run '^TestStage24'

echo '=== fuzz (3 seconds each, 2 workers) ==='
GOMAXPROCS=2 go test ./reliable -run '^$' -fuzz '^FuzzDecode$' -fuzztime=3s -parallel=2
GOMAXPROCS=2 go test ./reliable -run '^$' -fuzz '^FuzzPacketReceipt$' -fuzztime=3s -parallel=2
GOMAXPROCS=2 go test ./secureudp -run '^$' -fuzz '^FuzzAuditOpen$' -fuzztime=3s -parallel=2

echo '=== Linux amd64 and Windows amd64 builds ==='
mkdir -p bin
for tool in srdt-server srdt-client srdt-test srdt-load srdt-upstream-echo; do
  GOOS=linux GOARCH=amd64 go build -trimpath -o "bin/${tool}-linux-amd64" "./cmd/${tool}"
  CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -trimpath -o "bin/${tool}-windows-amd64.exe" "./cmd/${tool}"
done

echo '=== PASS ==='
