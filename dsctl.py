#!/usr/bin/env python

# Copyright (c) 2021 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

import json
import os
from dataclasses import dataclass
from json import JSONDecodeError
from os.path import join, dirname
import logging
import sys
import argparse

from dotenv import load_dotenv
from requests import post, RequestException

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@dataclass
class DataStructure:
    vendor: str
    name: str
    format: str


@dataclass
class Version:
    model: int
    revision: int
    addition: int


@dataclass
class Deployment:
    data_structure: DataStructure
    version: Version


def get_token():
    data = {
        "grant_type": "password",
        "username": os.environ["INSIGHTS_USERNAME"],
        "password": os.environ["INSIGHTS_PASSWORD"],
        "audience": "https://snowplowanalytics.com/api/",
        "client_id": os.environ["INSIGHTS_CLIENT_ID"],
        "client_secret": os.environ["INSIGHTS_CLIENT_SECRET"]
    }

    try:
        return post(
            "https://id.snowplowanalytics.com/oauth/token",
            json=data,
            headers={"Content-Type": "application/json"}
        ).json()["access_token"]
    except RequestException as e:
        logger.error("Could not contact authentication provider: {}".format(e))
        sys.exit(1)
    except JSONDecodeError:
        logger.error("Authentication provider did not return JSON content")
        sys.exit(1)
    except KeyError:
        logger.error("Authentication provider did not return an access token")
        sys.exit(1)


def get_base_path():
    return "https://console.snowplowanalytics.com/api/schemas/v1/organizations/{}".format(
        os.environ['CONSOLE_ORGANIZATION_ID']
    )


def get_base_headers(token):
    return {
        "Authorization": "Bearer {}".format(token)
    }


def handle_response(response, action):
    if response.ok:
        j = response.json()
        if not j['success']:
            logger.error("Data structure {} failed: {}".format(action, j['errors']))
            sys.exit(1)
    else:
        logger.error("Data structure {} failed: {}".format(action, response.text))
        sys.exit(1)


def validate(schema, token, stype, contains_meta):
    if stype not in ('event', 'entity'):
        logger.error('Schema type must be either "event" or "entity"')
        sys.exit(1)

    response = post(
        "{}/validation-requests/sync".format(get_base_path()),
        json={
            "meta": {
                "hidden": False,
                "schemaType": stype,
                "customData": {}
            },
            "data": schema
        } if not contains_meta else schema,
        headers=get_base_headers(token)
    )

    handle_response(response, 'validation')


def promote(spec, token, message, prod=False):
    response = post(
        "{}/deployment-requests/sync".format(get_base_path()),
        json={
            "name": spec.data_structure.name,
            "vendor": spec.data_structure.vendor,
            "format": spec.data_structure.format,
            "version": "{}-{}-{}".format(spec.version.model, spec.version.revision, spec.version.addition),
            "source": "VALIDATED" if not prod else "DEV",
            "target": "DEV" if not prod else "PROD",
            "message": message
        },
        headers=get_base_headers(token)
    )

    handle_response(response, 'promotion')


def resolve(schema, includes_meta):
    try:
        _self = schema['self'] if not includes_meta else schema['data']['self']
        vendor = _self['vendor']
        name = _self['name']
        formatx = _self['format']
        version = _self['version']
        ds = DataStructure(vendor, name, formatx)
        v = Version(*version.split('-'))
        return Deployment(ds, v)
    except ValueError:
        logger.error("Data structure spec is incorrect: Vendor, name, format or version is invalid")
        sys.exit(1)
    except KeyError:
        logger.error("Data structure does not include a 'self' element")
        sys.exit(1)


if __name__ == "__main__":
    dotenv_path = join(dirname(__file__), '.env')
    load_dotenv(dotenv_path)

    parser = argparse.ArgumentParser()
    parser.add_argument("--token-only", action="store_true", help="only get an access token and print it on stdout")
    parser.add_argument("--token", type=str, help="use this token to authenticate")
    parser.add_argument("--file", type=str, help="read schema from file (absolute path) instead of stdin")
    parser.add_argument("--type", choices=('event', 'entity'), help="document type")
    parser.add_argument("--includes-meta", action="store_true",
                        help="the input document already contains the meta field")
    parser.add_argument("--promote-to-dev", action="store_true",
                        help="promote from validated to dev; reads parameters from stdin or schema file")
    parser.add_argument("--promote-to-prod", action="store_true",
                        help="promote from dev to prod; reads parameters from stdin or schema file")
    parser.add_argument("--message", type=str, help="message to add to version deployment")
    args = parser.parse_args()

    if args.token_only:
        sys.stdout.write(get_token())
    else:
        token = args.token if args.token else get_token()
        message = args.message if args.message else "No message provided"

        if args.file:
            with open(args.file) as f:
                schema = json.load(f)
        else:
            schema = json.load(sys.stdin)

        if args.promote_to_dev:
            spec = resolve(schema, args.includes_meta)
            promote(spec, token, message)
        elif args.promote_to_prod:
            spec = resolve(schema, args.includes_meta)
            promote(spec, token, message, prod=True)
        else:
            schemaType = args.type if args.type else "event"
            validate(schema, token, schemaType, args.includes_meta)
