#!/bin/sh
# Runs three dstore nodes over real iroh endpoints on the loopback
# interface (no relays), pushes a tree, reads it back, runs a GC cycle.
# Everything lives under a temporary directory that is removed on exit.
set -eu
W=$(mktemp -d "${TMPDIR:-/tmp}/dstore-e2e.XXXXXX")
cleanup() { pkill -P $$ 2>/dev/null || true; rm -rf "$W"; }
trap cleanup EXIT INT TERM
cd "$(dirname "$0")/.."
go build -o "$W/dstore" ./cmd/dstore
B="$W/dstore"
cd "$W"
export DSTORE_LOG_LEVEL=${DSTORE_LOG_LEVEL:-warn}
mkdir -p src/sub local1 local2
i=1; while [ $i -le 12 ]; do head -c $((20000 + i*911)) /dev/urandom > src/f$i.bin; i=$((i+1)); done
head -c 30000 /dev/urandom > src/sub/g.bin
echo "hello dstore" > src/hello.txt

$B cluster init --store n1 --replicas 3 --weight 100 --no-relay --loopback > init.out
T=$(grep 'cluster ticket:' init.out | awk '{print $3}')
export DSTORE_TICKET=$T
$B serve --store n1 --no-relay --loopback --gc-interval 1m > n1.log 2>&1 &
sleep 2
TOK2=$($B token create --no-relay)
$B node join --store n2 --seed "$T" --token "$TOK2" --weight 100 --no-ramp --no-relay --loopback > n2.log 2>&1 &
sleep 2
TOK3=$($B token create --no-relay)
$B node join --store n3 --seed "$T" --token "$TOK3" --weight 100 --no-ramp --no-relay --loopback > n3.log 2>&1 &
i=0; while [ $i -lt 150 ]; do
  $B cluster status --no-relay > status.out 2>&1 || true
  if grep -q 'nodes 3 voters 3' status.out && ! grep -q '^transition ' status.out; then break; fi
  sleep 2; i=$((i+1))
done
cat status.out
grep -q 'nodes 3 voters 3' status.out || { echo "cluster did not reach 3 nodes / 3 voters"; cat n2.log n3.log; exit 1; }

$B push --local local1 --user e2e --no-relay src trees/demo
$B refs --no-relay
$B ls --no-relay trees/demo sub | grep -q g.bin
[ "$($B cat --no-relay trees/demo hello.txt)" = "hello dstore" ]
$B pull --local local2 --no-relay trees/demo
$B ref get --no-relay trees/demo
$B gc run --no-relay
$B ref delete --no-relay trees/demo
$B refs --no-relay | grep -qv '^trees/demo' 
echo "E2E OK"
