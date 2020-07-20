#!/usr/bin/env python
#
# Copyright 2020 Cloudera, Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import json
import re
import requests
import sys
from getpass import getpass

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

SRC_NR_API_ENDPOINT = sys.argv[1].rstrip('/')
BUCKET_ID = sys.argv[2]
FLOW_ID = sys.argv[3]
TGT_NR_API_ENDPOINT = sys.argv[4].rstrip('/')

sys.stdout.write("Source Username: ")
sys.stdout.flush()
src_username = sys.stdin.readline().rstrip()
src_password = getpass("Source Password: ").rstrip()

sys.stdout.write("Target Username: ")
sys.stdout.flush()
tgt_username = sys.stdin.readline().rstrip()
tgt_password = getpass("Target Password: ").rstrip()

src = requests.Session()
src.auth = (src_username, src_password)
src.headers.update({'Content-Type': 'application/json', 'Referer': SRC_NR_API_ENDPOINT})

tgt = requests.Session()
tgt.auth = (tgt_username, tgt_password)
tgt.headers.update({'Content-Type': 'application/json', 'Referer': TGT_NR_API_ENDPOINT})

def api_get(s, path, quiet=False):
    base_url = s.headers['Referer']
    url = '%s/%s' % (base_url, path)
    resp = s.get(url)
    if resp.status_code == requests.codes.ok:
        return resp.json()
    if not quiet:
        LOG.warn('GET %s returned %s' % (url, resp))
    return None

def api_post(s, path, payload, quiet=False):
    base_url = s.headers['Referer']
    url = '%s/%s' % (base_url, path)
    resp = s.post(url, json=payload)
    if resp.status_code == requests.codes.ok:
        return resp.json()
    if not quiet:
        LOG.warn('POST %s returned %s' % (url, resp))
    return None

def json_replace(pattern, replacement, j):
    return json.loads(re.sub(pattern, replacement, json.dumps(j)))

def get_bucket(s, identifier, method='id', quiet=False):
    base_url = s.headers['Referer']
    if method == 'id':
        return api_get(s, 'buckets/%s' % (identifier,), quiet)
    elif method == 'name':
        buckets = api_get(s, 'buckets', quiet)
        if buckets:
            items = [b for b in buckets if b['name'] == identifier]
            if items:
                return items[0]
    else:
        raise RuntimeError('Invalid method: %s' % (method,))
    return None

def create_bucket(s, payload):
    base_url = s.headers['Referer']
    return api_post(s, 'buckets', payload)

def get_flow(s, bucket_id, identifier, method='id', quiet=False):
    base_url = s.headers['Referer']
    if method == 'id':
        return api_get(s, 'buckets/%s/flows/%s' % (bucket_id, identifier), quiet)
    elif method == 'name':
        flows = api_get(s, 'buckets/%s/flows' % (bucket_id,), quiet)
        if flows:
            items = [b for b in flows if b['name'] == identifier]
            if items:
                return items[0]
    else:
        raise RuntimeError('Invalid method: %s' % (method,))
    return None

def create_flow(s, bucket_id, payload):
    base_url = s.headers['Referer']
    return api_post(s, 'buckets/%s/flows' % (bucket_id,), payload)

def get_version(s, bucket_id, flow_id, version, quiet=False):
    base_url = s.headers['Referer']
    return api_get(s, 'buckets/%s/flows/%s/versions/%s' % (bucket_id, flow_id, version), quiet=True)

def create_version(s, bucket_id, flow_id, payload):
    base_url = s.headers['Referer']
    return api_post(s, 'buckets/%s/flows/%s/versions' % (bucket_id, flow_id), payload)

# Bucket

src_bucket = get_bucket(src, BUCKET_ID)
tgt_bucket = get_bucket(tgt, src_bucket['name'], method='name', quiet=True)
if tgt_bucket:
    print('Bucket already exists on the target')
else:
    print('Creating bucket')
    tgt_bucket = create_bucket(tgt, src_bucket)
print('  Bucket: %s (%s)' % (tgt_bucket['name'], tgt_bucket['identifier']))

# Flow

src_flow = json_replace(src_bucket['identifier'], tgt_bucket['identifier'], get_flow(src, BUCKET_ID, FLOW_ID))
tgt_flow = get_flow(tgt, tgt_bucket['identifier'], src_flow['name'], method='name', quiet=True)
if tgt_flow:
    print('Flow already exists on the target')
else:
    print('Creating flow')
    tgt_flow = create_flow(tgt, tgt_bucket['identifier'], src_flow)
print('  Flow: %s (%s)' % (tgt_flow['name'], tgt_flow['identifier']))

# Version

version_count = src_flow['versionCount']
for version in range(1, version_count + 1):
    payload = get_version(src, src_bucket['identifier'], src_flow['identifier'], version)
    payload = json_replace(src_bucket['identifier'], tgt_bucket['identifier'], payload)
    payload = json_replace(src_flow['identifier'], tgt_flow['identifier'], payload)
    tgt_version = get_version(tgt, tgt_bucket['identifier'], tgt_flow['identifier'], version, quiet=True)
    if tgt_version:
        print('Version %d already exists on the target' % (version,))
    else:
        print('Creating version %d' % (version,))
        tgt_version = create_version(tgt, tgt_bucket['identifier'], tgt_flow['identifier'], payload)
        print('  Version: %d' % (tgt_version['snapshotMetadata']['version'],))
