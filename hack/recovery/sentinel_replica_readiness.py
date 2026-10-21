"""Read-only lab gate for Sentinel's observations of current Redis replicas."""
import json
import re
import sys


def verify(records, expected, primary_addresses):
    if not isinstance(records, list) or len(expected) != 2:
        raise ValueError("invalid replica inventory")
    aliases = {}
    identities = set()
    names = set()
    for peer in expected:
        if not re.fullmatch(r"[0-9a-f]{40}", peer["runid"]) or peer["runid"] in identities:
            raise ValueError("invalid or duplicate live replica identity")
        identities.add(peer["runid"])
        if peer["name"] in names:
            raise ValueError("duplicate live replica name")
        names.add(peer["name"])
        for address in peer["addresses"]:
            if not address or address in aliases:
                raise ValueError("ambiguous live replica address")
            aliases[address] = peer
    ready = set()
    for record in records:
        if not isinstance(record, dict) or not isinstance(record.get("flags"), str):
            raise ValueError("malformed Sentinel replica observation")
        flags = set(record["flags"].split(","))
        peer = aliases.get(record.get("ip"))
        if peer is None:
            # Retain stale configuration rather than RESET election state, but
            # never accept a live unknown replica which Sentinel could promote.
            if flags & {"s_down", "o_down", "disconnected"}:
                continue
            raise ValueError("Sentinel reports an unknown non-down replica")
        if (record.get("port") != "6379" or record.get("runid") != peer["runid"]
                or flags != {"slave"} or record.get("role-reported") != "slave"
                or record.get("master-host") not in primary_addresses
                or record.get("master-port") != "6379"
                or record.get("master-link-status") != "ok"
                or record.get("replica-announced") != "1"):
            raise ValueError("current replica identity or replication link is not ready")
        numbers = {}
        for key in ("slave-priority", "info-refresh", "last-ok-ping-reply", "down-after-milliseconds"):
            value = record.get(key)
            if not isinstance(value, str) or not re.fullmatch(r"[0-9]+", value):
                raise ValueError("missing or malformed replica eligibility metadata")
            numbers[key] = int(value)
        # Sentinel normally refreshes INFO every 10s. Allow bounded scheduling
        # jitter, not absent observations (zero) or indefinitely stale INFO.
        if (numbers["slave-priority"] <= 0 or not 0 < numbers["info-refresh"] <= 15000
                or numbers["down-after-milliseconds"] <= 0
                or numbers["last-ok-ping-reply"] >= numbers["down-after-milliseconds"]):
            raise ValueError("current replica is not fresh and eligible for election")
        ready.add(peer["name"])
    if ready != names:
        raise ValueError("Sentinel has not observed every current replica as eligible")


if __name__ == "__main__":
    verify(json.load(sys.stdin), json.loads(sys.argv[1]), json.loads(sys.argv[2]))
