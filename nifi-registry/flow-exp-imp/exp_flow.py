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
import os
import re
import requests
import sys
from getpass import getpass

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

SRC_NR_API_ENDPOINT = sys.argv[1].rstrip('/')
BUCKET_ID = sys.argv[2]
FLOW_ID = sys.argv[3]
SAVE_DIR = sys.argv[4]

if os.path.exists(SAVE_DIR):
    LOG.error("Directory %s already exists", SAVE_DIR)
    exit(1)
os.makedirs(SAVE_DIR)

sys.stdout.write("Source Username: ")
sys.stdout.flush()
src_username = sys.stdin.readline().rstrip()
src_password = getpass("Source Password: ").rstrip()

src = requests.Session()
src.auth = (src_username, src_password)
src.headers.update({'Content-Type': 'application/json', 'Referer': SRC_NR_API_ENDPOINT})

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
assert src_bucket, "Bucket with ID %s not found" % (BUCKET_ID,)
open(os.path.join(SAVE_DIR, 'bucket.json'), 'w').write(json.dumps(src_bucket, indent=2))
LOG.info('Exported bucket: %s (%s)' % (src_bucket['name'], src_bucket['identifier']))

# Flow

src_flow = get_flow(src, BUCKET_ID, FLOW_ID)
assert src_flow, "Flow with ID %s not found" % (FLOW_ID,)
open(os.path.join(SAVE_DIR, 'flow.json'), 'w').write(json.dumps(src_flow, indent=2))
LOG.info('Exported flow: %s (%s)' % (src_flow['name'], src_flow['identifier']))

# Version

version_count = src_flow['versionCount']
for version in range(1, version_count + 1):
    src_version = get_version(src, src_bucket['identifier'], src_flow['identifier'], version)
    open(os.path.join(SAVE_DIR, 'version_%s.json' % (version,)), 'w').write(json.dumps(src_version, indent=2))
    LOG.info('Exported version: %s' % (version,))
