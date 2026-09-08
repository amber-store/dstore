#!/bin/sh
# Entrypoint of the dstore node image as the operator runs it. The
# operator gives every node its identity, its address and its role; on a
# fresh store the role decides whether the node creates the cluster or
# joins one, and afterwards the node just serves.
set -eu
STORE=${DSTORE_STORE:-/data}
PORT=${DSTORE_PORT:-4433}
mkdir -p "$STORE"
if [ ! -f "$STORE/identity" ] && [ -n "${DSTORE_IDENTITY:-}" ]; then
  umask 077
  printf '%s\n' "$DSTORE_IDENTITY" > "$STORE/identity"
  umask 022
fi
set -- --store "$STORE" --bind "0.0.0.0:$PORT" --no-relay
if [ -n "${DSTORE_ADVERTISE:-}" ]; then set -- "$@" --advertise-addr "$DSTORE_ADVERTISE"; fi
if [ -n "${DSTORE_PAXOS_DIR:-}" ]; then set -- "$@" --paxos-dir "$DSTORE_PAXOS_DIR"; fi
if [ -n "${DSTORE_GC_INTERVAL:-}" ]; then set -- "$@" --gc-interval "$DSTORE_GC_INTERVAL"; fi
# shellcheck disable=SC2086
set -- "$@" ${DSTORE_EXTRA_ARGS:-}
WEIGHT=${DSTORE_WEIGHT:-auto}

# A store that already belongs to a cluster just serves.
if [ -f "$STORE/identity" ] && dstore cluster ticket --store "$STORE" >/dev/null 2>&1; then
  exec dstore serve "$@"
fi
case "${DSTORE_ROLE:-}" in
  init)
    MINR=""
    if [ "${DSTORE_MIN_REPLICAS:-0}" != "0" ]; then MINR="--min-replicas $DSTORE_MIN_REPLICAS"; fi
    ZONE=""
    if [ -n "${DSTORE_ZONE:-}" ]; then ZONE="--zone $DSTORE_ZONE"; fi
    # shellcheck disable=SC2086
    dstore cluster init "$@" --replicas "${DSTORE_REPLICAS:-3}" $MINR --weight "$WEIGHT" $ZONE
    exec dstore serve "$@"
    ;;
  join)
    : "${DSTORE_SEED:?join needs DSTORE_SEED}" "${DSTORE_TOKEN:?join needs DSTORE_TOKEN}"
    ZONE=""
    if [ -n "${DSTORE_ZONE:-}" ]; then ZONE="--zone $DSTORE_ZONE"; fi
    # node join keeps serving after it has joined.
    # shellcheck disable=SC2086
    exec dstore node join "$@" --seed "$DSTORE_SEED" --token "$DSTORE_TOKEN" --weight "$WEIGHT" --no-ramp $ZONE
    ;;
  *)
    echo "store is not a cluster member and DSTORE_ROLE is not init or join" >&2
    exit 1
    ;;
esac
