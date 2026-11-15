#!/usr/bin/env python3
"""One-time NiFi 2.x flow import/enable on a NEW local instance.

Expects installed `requests`, and NIFI_USERNAME / NIFI_PASSWORD in the environment.
Aborts if root contains any unexpected process groups to avoid destructive changes.
Never deletes components or data.
"""
import os
from pathlib import Path
import sys
import time
from uuid import uuid4

try:
    import requests
    import urllib3
except ImportError:
    raise SystemExit("Install requests: python3 -m pip install requests")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
URL = os.environ.get("NIFI_API_URL", "https://localhost:8443/nifi-api").rstrip("/")
USERNAME = os.environ.get("NIFI_USERNAME")
PASSWORD = os.environ.get("NIFI_PASSWORD")
FLOW = Path(__file__).resolve().parent.parent / "nifi/flows/warsaw-ingestion.json"
if not USERNAME or not PASSWORD:
    raise SystemExit("Set NIFI_USERNAME / NIFI_PASSWORD in .env")
if not FLOW.is_file():
    raise SystemExit(f"Missing flow definition: {FLOW}")

client = requests.Session()
client.verify = False  # localhost self-signed development TLS only


def req(method, path, **kwargs):
    r = client.request(method, URL + path, timeout=120, **kwargs)
    if not r.ok:
        raise RuntimeError(f"NiFi {method} {path}: HTTP {r.status_code}: {r.text[:1000]}")
    return r.json() if r.content and 'json' in r.headers.get('Content-Type', '') else r.text


print("Waiting for NiFi to accept authentication...")
for attempt in range(90):
    try:
        token = req("POST", "/access/token", data={"username": USERNAME, "password": PASSWORD})
        client.headers.update({"Authorization": f"Bearer {token}"})
        break
    except (requests.RequestException, RuntimeError):
        if attempt == 89:
            raise SystemExit("NiFi not ready or credentials invalid; see docker compose logs nifi")
        time.sleep(3)

root_flow = req("GET", "/flow/process-groups/root")
root_id = root_flow["processGroupFlow"]["id"]
children = root_flow["processGroupFlow"]["flow"].get("processGroups", [])
if children:
    existing = [g.get("component", {}).get("name") for g in children]
    if existing != ["NiFi Flow"]:
        raise SystemExit(f"Refusing import into nonempty root: {existing}")
    group_id = children[0]["id"]
    print(f"Flow already present: {group_id}; skipping import.")
else:
    print("Importing exported NiFi Flow...")
    with FLOW.open("rb") as f:
        imported = req(
            "POST", f"/process-groups/{root_id}/process-groups/upload",
            data={"groupName": "NiFi Flow", "positionX": "0", "positionY": "0", "clientId": str(uuid4())},
            files={"file": (FLOW.name, f, "application/json")},
        )
    group_id = imported.get("id") or imported.get("component", {}).get("id")
    if not group_id:
        raise SystemExit(f"Import response did not include process group ID: {imported}")
    print(f"Imported process group {group_id}.")

print("Enabling Controller Services...")
req("PUT", f"/flow/process-groups/{group_id}/controller-services",
    json={"id": group_id, "state": "ENABLED"})
for attempt in range(45):
    data = req("GET", f"/flow/process-groups/{group_id}/controller-services",
               params={"includeAncestorGroups": "false", "includeDescendantGroups": "false"})
    services = data.get("controllerServices", [])
    states = [(s.get("component", {}).get("name"), s.get("component", {}).get("state")) for s in services]
    if services and all(state == "ENABLED" for _, state in states):
        break
    if attempt == 44:
        raise SystemExit(f"Controller Services did not all enable: {states}; check NiFi UI.")
    time.sleep(2)
print("Services enabled:", states)

# Schedule processors in each of the 4 imported subgroups. Do not claim success without checks.
group_flow = req("GET", f"/flow/process-groups/{group_id}")
subgroups = group_flow["processGroupFlow"]["flow"].get("processGroups", [])
required = {"GPS Ingestion", "Weather Ingestion", "Air Quality Ingestion", "GTFS Ingestion"}
actual = {g.get("component", {}).get("name") for g in subgroups}
if actual != required:
    raise SystemExit(f"Missing or unexpected process groups: {actual} vs {required}")
for group in subgroups:
    cid = group["id"]
    name = group["component"]["name"]
    print(f"Starting {name}...")
    req("PUT", f"/flow/process-groups/{cid}", json={"id": cid, "state": "RUNNING"})
print("NiFi start requests submitted. Verify processors are Running and Valid in the UI.")
