#!/usr/bin/env python

# Copyright (c) 2022 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

import os
from dataclasses import dataclass
from enum import Enum
from json import JSONDecodeError, dumps, load
from os.path import join, dirname
import logging
import sys
import argparse
from typing import Dict, Literal, Optional, TextIO, cast

from dotenv import load_dotenv
from requests import get, post, RequestException, Response

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)


class CLIArguments(argparse.Namespace):
    token_only: bool
    token: str | None
    file: TextIO
    type: Literal["event", "entity"]
    includes_meta: bool
    promote_to_dev: bool
    promote_to_prod: bool
    allow_patch: bool
    message: str | None


@dataclass
class Config:
    console_host: str
    organization_id: str
    api_key: str
    base_url: str
    ds_url: str


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

    def __str__(self) -> str:
        return f"{self.model}-{self.revision}-{self.addition}"


@dataclass
class Deployment:
    data_structure: DataStructure
    version: Version


class SchemaType(str, Enum):
    EVENT = 'event'
    ENTITY = 'entity'


def get_config() -> Optional[Config]:
    """Returns an endpoint configuration object"""

    try:
        org_id = os.environ['CONSOLE_ORGANIZATION_ID']
        api_key = os.environ['CONSOLE_API_KEY']
    except KeyError:
        logger.error("Environment variables CONSOLE_ORGANIZATION_ID and/or CONSOLE_API_KEY are not set")
        return None

    host = os.environ.get('CONSOLE_HOST', 'console')
    base_url = f"https://{host}.snowplowanalytics.com/api/msc/v1/organizations/{org_id}"

    return Config(
        console_host=host,
        organization_id=org_id,
        api_key=api_key,
        base_url=base_url,
        ds_url=f"{base_url}/data-structures/v1"
    )


def get_token(config: Config) -> Optional[str]:
    """
    Retrieves a JWT from BDP Console.

    :return: The token
    """
    response = None
    body = None
    try:
        response = get(
            f"{config.base_url}/credentials/v2/token",
            headers={"X-API-Key": config.api_key}
        )
        body = response.json()
        if not isinstance(body, dict):
            raise TypeError()
        return cast(str, body["accessToken"])
    except RequestException as e:
        logger.error(f"Could not contact BDP Console: {e}")
        return None
    except JSONDecodeError:
        logger.error(f"get_token: Response was not valid JSON: {response and response.text}")
        return None
    except (KeyError, TypeError):
        logger.error(f"get_token: Invalid response body: {dumps(body, indent=2)}")
        return None


def get_base_headers(auth_token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {auth_token}"
    }


def handle_response(response: Response, action: str) -> bool:
    """
    Generic response handler for validation and promotion operations. Confirms that it all went well.

    :param response: The Response object to operate on
    :param action: The action ('validation' or 'promotion') that created the Response object
    :return: None
    """
    if response.ok:
        try:
            body = response.json()
            if not isinstance(body, dict) or not body.get("success"):
                logger.error(f"Data structure {action} failed: {body}")
                return False
            return True
        except JSONDecodeError:
            logger.error(f"handle_response: Response was not valid JSON: {response.text}")
            return False
    else:
        logger.error(f"Data structure {action} failed: {response.text}")
        return False


def validate(config: Config, data_structure: dict, auth_token: str, stype: str, contains_meta: bool) -> bool:
    """
    Validates a data structure against the BDP API.

    :param config: Endpoint configuration object
    :param data_structure: A dictionary representing the data structure
    :param auth_token: The JWT to use
    :param stype: The type of the data structure (event or entity)
    :param contains_meta: A flag to indicate whether the `meta` section already exists in the dictionary
    :return:
    """
    if stype not in (SchemaType.EVENT, SchemaType.ENTITY):
        logger.error('Data structure type must be either "event" or "entity"')
        return False

    try:
        response = post(
            f"{config.ds_url}/validation-requests",
            json={
                "meta": {
                    "hidden": False,
                    "schemaType": stype,
                    "customData": {}
                },
                "data": data_structure
            } if not contains_meta else data_structure,
            headers=get_base_headers(auth_token)
        )
    except RequestException as e:
        logger.error(f"Could not contact BDP Console: {e}")
        return False

    return handle_response(response, 'validation')


def promote(
    config: Config,
    deployment: Deployment,
    auth_token: str,
    deployment_message: str,
    to_production: bool = False,
    request_patch: bool = False,
) -> bool:
    """
    Promotes a data structure to staging or production.

    :param config: Endpoint configuration object
    :param deployment: The Deployment class to use
    :param auth_token: The JWT to use
    :param deployment_message: A message describing the changes applied to the data structure
    :param to_production: A flag to indicate if the data structure should be deployed to production (default: staging)
    :param request_patch: A flag to indicate if the data structure deployment should request patch support (default: False)
    :return: None
    """
    try:
        response = post(
            f"{config.ds_url}/deployment-requests",
            json={
                "name": deployment.data_structure.name,
                "vendor": deployment.data_structure.vendor,
                "format": deployment.data_structure.format,
                "version": "{}-{}-{}".format(deployment.version.model, deployment.version.revision,
                                             deployment.version.addition),
                "source": "VALIDATED" if not to_production else "DEV",
                "target": "DEV" if not to_production else "PROD",
                "message": deployment_message
            },
            params=dict(patch=request_patch),
            headers=get_base_headers(auth_token),
        )
    except RequestException as e:
        logger.error(f"Could not contact BDP Console: {e}")
        return False

    return handle_response(response, 'promotion')


def resolve(data_structure: dict | None, includes_meta: bool) -> Optional[Deployment]:
    """
    Reads a data structure and extracts the self-describing section.

    :param data_structure: A dictionary representing the data structure
    :param includes_meta: A flag to indicate whether the `meta` section already exists in the dictionary
    :return: A Deployment instance
    """
    try:
        if not isinstance(data_structure, dict):
            raise TypeError()
        _self = (
            data_structure["self"]
            if not includes_meta
            else data_structure["data"]["self"]
        )
        vendor = _self['vendor']
        name = _self['name']
        ds_format = _self['format']
        version = _self['version']
        ds = DataStructure(vendor, name, ds_format)
        v = Version(*version.split('-'))
        return Deployment(ds, v)
    except (ValueError, TypeError):
        logger.error("Data structure spec is incorrect: Vendor, name, format or version is invalid")
        return None
    except KeyError:
        logger.error("Data structure does not include a correct 'self' element")
        return None


def parse_arguments() -> CLIArguments:
    """Parses and returns CLI parameters"""

    parser = argparse.ArgumentParser()
    parser.add_argument("--token-only", action="store_true", help="only get an access token and print it on stdout")
    parser.add_argument("--token", type=str, help="use this token to authenticate")
    parser.add_argument(
        "--file",
        type=argparse.FileType(),
        default="-",
        help="read data structure from file (absolute path) instead of stdin",
    )
    parser.add_argument(
        "--type", choices=("event", "entity"), default="event", help="document type"
    )
    parser.add_argument("--includes-meta", action="store_true",
                        help="the input document already contains the meta field")
    parser.add_argument("--promote-to-dev", action="store_true",
                        help="promote from validated to dev; reads parameters from stdin or --file parameter")
    parser.add_argument("--promote-to-prod", action="store_true",
                        help="promote from dev to prod; reads parameters from stdin or --file parameter")
    parser.add_argument(
        "--allow-patch",
        action="store_true",
        help="request patch support in promotion request",
    )
    parser.add_argument("--message", type=str, help="message to add to version deployment")

    return parser.parse_args(namespace=CLIArguments())


def parse_input_file(file: TextIO) -> Optional[dict]:
    """
    Loads schema from a file or standard input.

    :param file: File to read from
    :return: The schema JSON
    """
    try:
        return load(file)
    except JSONDecodeError as e:
        logger.error(f"Provided input is not valid JSON: {e}")
        return None
    except Exception as e:
        logger.error(f"Could not read {file.name if file.name else 'stdin'}: {e}")
        return None
    finally:
        file.close()


def flow(args: CLIArguments, config: Config) -> bool:
    """Main operation actually invoking the DS API to validate or promote a data structure"""

    message = args.message if args.message else "No message provided"
    token = args.token if args.token else get_token(config)
    schema = parse_input_file(args.file)
    schema_type = args.type
    spec = resolve(schema, args.includes_meta)

    if not token or not schema or not spec:
        return False

    if args.promote_to_dev or args.promote_to_prod:
        return promote(
            config,
            spec,
            token,
            message,
            to_production=args.promote_to_prod,
            request_patch=args.allow_patch,
        )
    else:
        return validate(config, schema, token, schema_type, args.includes_meta)


def main() -> None:
    arguments = parse_arguments()
    config = get_config()

    if not config:
        sys.exit(1)

    if arguments.token_only:
        token = get_token(config)
        if not token:
            sys.exit(1)
        sys.stdout.write(token)
    else:
        if not flow(arguments, config):
            sys.exit(1)


if __name__ == "__main__":
    main()
