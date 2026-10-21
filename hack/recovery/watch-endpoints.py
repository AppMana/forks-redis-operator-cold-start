#!/usr/bin/env python3
"""Bounded local-only EndpointSlice watch; never mutates Kubernetes objects."""
import argparse
import json
import os
import queue
import subprocess
import threading
import time

parser = argparse.ArgumentParser()
parser.add_argument("kubeconfig")
parser.add_argument("--seconds", type=int, default=180)
args = parser.parse_args()
assert 1 <= args.seconds <= 600
namespace = os.environ.get("CANARY_NAMESPACE", "redis-upgrade-canary-persistent")
assert namespace.startswith("redis-upgrade-canary")
base = ["kubectl", "--kubeconfig", args.kubeconfig]
context = subprocess.check_output(base + ["config", "current-context"], text=True).strip()
assert context.startswith("kind-redis-recovery-")
server = subprocess.check_output(base + ["config", "view", "--minify", "-o",
                                         "jsonpath={.clusters[0].cluster.server}"], text=True)
assert server.startswith("https://127.0.0.1:")
command = base + ["--context", context, "-n", namespace, "get", "endpointslices",
                  "-l", "kubernetes.io/service-name=upgrade-master", "--watch",
                  "--output-watch-events", "-o", "json"]
events = queue.Queue()
watch = subprocess.Popen(command, stdout=subprocess.PIPE)
decode = subprocess.Popen(["jq", "--unbuffered", "-c", "."], stdin=watch.stdout,
                          stdout=subprocess.PIPE, text=True)
watch.stdout.close()


def read_events():
    for line in decode.stdout:
        events.put(line)
    events.put(None)


threading.Thread(target=read_events, daemon=True).start()
slices = {}
count = 0
deadline = time.monotonic() + args.seconds
try:
    while time.monotonic() < deadline:
        try:
            line = events.get(timeout=min(1, max(0.001, deadline - time.monotonic())))
        except queue.Empty:
            continue
        if line is None:
            raise RuntimeError("EndpointSlice watch ended before the acceptance deadline")
        event = json.loads(line)
        kind, obj = event["type"], event["object"]
        if kind == "BOOKMARK":
            continue
        if kind not in ("ADDED", "MODIFIED", "DELETED"):
            raise RuntimeError("Unexpected watch event: " + kind)
        uid = obj["metadata"]["uid"]
        if kind == "DELETED":
            slices.pop(uid, None)
        else:
            slices[uid] = obj
        ready = set()
        for item in slices.values():
            for endpoint in item.get("endpoints") or []:
                conditions = endpoint.get("conditions", {})
                # Missing readiness is interpreted as ready by API consumers.
                if conditions.get("ready") is not False and not conditions.get("terminating"):
                    ready.update(endpoint.get("addresses", []))
        if len(ready) > 1:
            raise RuntimeError("Multiple ready writable addresses observed: " + repr(sorted(ready)))
        count += 1
    if count == 0:
        raise RuntimeError("No EndpointSlice events observed; cannot assert safety")
    print(f"PASS: {count} EndpointSlice events over {args.seconds}s; at most one ready writable address")
finally:
    for process in (watch, decode):
        process.terminate()
    for process in (watch, decode):
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
