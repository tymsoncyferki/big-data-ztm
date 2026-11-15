#!/usr/bin/env python3
"""One-time NiFi 2.x flow import/enable on a NEW local instance.

Runs in the nifi-init Docker image (requests is installed in that image).
Aborts if root contains any unexpected process groups to avoid destructive changes.
Never deletes components or data.
"""
import argparse
import os
from pathlib import Path
import sys
import time
from uuid import uuid4

try:
    import requests
    import urllib3
except ImportError:
    raise SystemExit("Missing requests in the nifi-init container image")

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


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--action', choices=['init', 'start-gtfs', 'enable-gtfs-trigger', 'stop-gtfs'], default='init')
parser.add_argument('--initial-download', action='store_true',
                    help='Run the GTFS trigger once before enabling its daily schedule')
args = parser.parse_args()
if args.initial_download and args.action != 'start-gtfs':
    parser.error('--initial-download requires --action start-gtfs')

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

def flow_group(import_if_missing: bool) -> str:
    root_flow = req('GET', '/flow/process-groups/root')
    root_id = root_flow['processGroupFlow']['id']
    children = root_flow['processGroupFlow']['flow'].get('processGroups', [])
    if children:
        existing = [g.get('component', {}).get('name') for g in children]
        if existing != ['NiFi Flow']:
            raise SystemExit(f'Refusing to manage unexpected root groups: {existing}')
        return children[0]['id']
    if not import_if_missing:
        raise SystemExit('NiFi Flow is missing. Run bootstrap first.')
    print('Importing exported NiFi Flow...')
    with FLOW.open('rb') as source:
        imported = req(
            'POST', f'/process-groups/{root_id}/process-groups/upload',
            data={'groupName': 'NiFi Flow', 'positionX': '0', 'positionY': '0',
                  'clientId': str(uuid4())},
            files={'file': (FLOW.name, source, 'application/json')},
        )
    group_id = imported.get('id') or imported.get('component', {}).get('id')
    if not group_id:
        raise SystemExit(f'Import response did not include process group ID: {imported}')
    print(f'Imported process group {group_id}.')
    return group_id


def subgroups(group_id: str) -> dict[str, str]:
    data = req('GET', f'/flow/process-groups/{group_id}')
    groups = data['processGroupFlow']['flow'].get('processGroups', [])
    names = {g['component']['name']: g['id'] for g in groups}
    expected = {'GPS Ingestion', 'Weather Ingestion', 'Air Quality Ingestion',
                'GTFS Ingestion'}
    if set(names) != expected:
        raise SystemExit(f'Unexpected process groups: {set(names)} (expected {expected})')
    return names


def change_processor(processor_id: str, state: str) -> None:
    current = req('GET', f'/processors/{processor_id}')
    if state != 'RUN_ONCE' and current['component'].get('state') == state:
        return
    req('PUT', f'/processors/{processor_id}/run-status', json={
        'revision': current['revision'], 'state': state,
    })


def gtfs_processors(group_id: str) -> tuple[list[str], str]:
    entities = req('GET', f'/process-groups/{group_id}/processors').get('processors', [])
    processors = {p['component']['name']: p['id'] for p in entities}
    expected = {'Generate GTFS Trigger', 'Download Warsaw GTFS',
                'Set GTFS Archive Attributes', 'Archive GTFS to HDFS',
                'Log Download Errors', 'Log HDFS Errors'}
    if set(processors) != expected:
        raise SystemExit(f'GTFS processors differ from flow definition: {set(processors)}')
    trigger = processors.pop('Generate GTFS Trigger')
    return list(processors.values()), trigger


def wait_stopped(processor_id: str) -> None:
    for _ in range(30):
        entity = req('GET', f'/processors/{processor_id}')
        if entity['component'].get('state') == 'STOPPED':
            return
        time.sleep(1)
    raise RuntimeError(f'Processor {processor_id} did not stop')


def enable_services(group_id: str) -> None:
    print('Enabling Controller Services...')
    req('PUT', f'/flow/process-groups/{group_id}/controller-services',
        json={'id': group_id, 'state': 'ENABLED'})
    for attempt in range(45):
        data = req('GET', f'/flow/process-groups/{group_id}/controller-services',
                   params={'includeAncestorGroups': 'false',
                           'includeDescendantGroups': 'false'})
        services = data.get('controllerServices', [])
        states = [(s.get('component', {}).get('name'),
                   s.get('component', {}).get('state')) for s in services]
        if services and all(state == 'ENABLED' for _, state in states):
            print('Services enabled:', states)
            return
        time.sleep(2)
    raise SystemExit(f'Controller Services did not all enable: {states}')


group_id = flow_group(import_if_missing=args.action == 'init')
groups = subgroups(group_id)
gtfs_id = groups['GTFS Ingestion']

if args.action == 'init':
    enable_services(group_id)
    # GTFS is opt-in, just like the simulator. In particular, a re-run of
    # bootstrap without --with-data must disable an old daily GTFS schedule.
    req('PUT', f'/flow/process-groups/{gtfs_id}',
        json={'id': gtfs_id, 'state': 'STOPPED'})
    print('GTFS download remains stopped until --with-data is selected.')
    for name, cid in groups.items():
        if name == 'GTFS Ingestion':
            continue
        print(f'Starting {name}...')
        req('PUT', f'/flow/process-groups/{cid}',
            json={'id': cid, 'state': 'RUNNING'})
    print('NiFi ingestion flows configured.')

elif args.action == 'stop-gtfs':
    req('PUT', f'/flow/process-groups/{gtfs_id}',
        json={'id': gtfs_id, 'state': 'STOPPED'})
    print('GTFS download flow stopped (daily trigger disabled).')

elif args.action == 'start-gtfs':
    workers, trigger = gtfs_processors(gtfs_id)
    # Start downstream processing before generating a FlowFile. Keep the
    # CRON-driven trigger stopped until the optional one-off run is issued.
    change_processor(trigger, 'STOPPED')
    wait_stopped(trigger)
    for processor_id in workers:
        change_processor(processor_id, 'RUNNING')
    if args.initial_download:
        print('Requesting one initial GTFS download through NiFi...')
        change_processor(trigger, 'RUN_ONCE')
        # GenerateFlowFile has a fast onTrigger; wait for its one-off cycle
        # to leave the STOPPED state before enabling daily CRON scheduling.
        wait_stopped(trigger)
        print('Initial GTFS trigger requested; daily schedule remains off until archival succeeds.')
    else:
        change_processor(trigger, 'RUNNING')
        print('GTFS daily download trigger enabled (03:00 Europe/Warsaw).')

elif args.action == 'enable-gtfs-trigger':
    # Only called after the host has observed a complete archive in HDFS.
    _, trigger = gtfs_processors(gtfs_id)
    change_processor(trigger, 'RUNNING')
    print('GTFS daily download trigger enabled (03:00 Europe/Warsaw).')
