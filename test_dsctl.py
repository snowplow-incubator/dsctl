import argparse
import io
import os
import sys
from copy import deepcopy
from json import JSONDecodeError, dumps

import pytest
import responses
from requests import Response
from responses import matchers

import dsctl


@pytest.fixture
def environment(mocker):
    mocker.patch.dict(
        os.environ, {
            'CONSOLE_ORGANIZATION_ID': 'CONSOLE_ID',
            'CONSOLE_API_KEY': 'api-key'
        }
    )


@pytest.fixture
def config(environment):
    return dsctl.get_config()


@pytest.fixture
def token_url(config):
    return f"{config.base_url}/credentials/v2/token"


@pytest.fixture
def throws_json_decode_error(mocker):
    def throw():
        raise JSONDecodeError('json decode error', "", 0)

    return mocker.Mock(side_effect=throw)


@pytest.fixture
def data_structure():
    return {
        "self": {
            "vendor": "com.snowplow",
            "name": "transaction",
            "format": "jsonschema",
            "version": "1-0-0"
        }
    }


@pytest.fixture
def data_structure_with_meta(data_structure):
    return {
        "meta": {
            "hidden": False,
            "schemaType": "event",
            "customData": {}
        },
        "data": data_structure
    }


@pytest.fixture
def deployment(data_structure):
    return dsctl.Deployment(
        data_structure=dsctl.DataStructure(
            vendor=data_structure['self']['vendor'],
            name=data_structure['self']['name'],
            format=data_structure['self']['format']
        ),
        version=dsctl.Version(
            *data_structure['self']['version'].split('-')
        )
    )


@pytest.fixture
def args(mocker, data_structure):
    args = mocker.Mock()
    mocker.patch.object(args, 'message', None)
    mocker.patch.object(args, 'token', None)
    mocker.patch.object(args, 'file', None)
    mocker.patch.object(sys, 'stdin', io.StringIO(dumps(data_structure)))
    mocker.patch.object(args, 'type', None)
    mocker.patch.object(args, 'includes_meta', False)

    return args


def test_config_no_env():
    assert dsctl.get_config() is None


@responses.activate
def test_get_token_connection_failure(config):
    assert dsctl.get_token(config) is None


@responses.activate
def test_get_token_status_failure(config, token_url):
    responses.add(responses.GET, token_url, status=403)
    assert dsctl.get_token(config) is None


@responses.activate
def test_get_token_json_failure(config, token_url):
    responses.add(responses.GET, token_url, json={}, status=200)
    assert dsctl.get_token(config) is None


@responses.activate
def test_get_token_success(config, token_url):
    responses.add(responses.GET, token_url, json={"accessToken": "abcd"}, status=200)
    assert dsctl.get_token(config) == "abcd"


def test_handle_response_not_ok(mocker):
    response = mocker.Mock(spec=Response)
    mocker.patch.object(response, 'ok', False)
    assert dsctl.handle_response(response, '') is False


def test_handle_response_not_json(mocker, throws_json_decode_error):
    response = mocker.Mock(spec=Response)
    mocker.patch.object(response, 'json', throws_json_decode_error)
    assert dsctl.handle_response(response, '') is False


def test_handle_response_not_valid_json(mocker):
    response = mocker.Mock(spec=Response)
    mocker.patch.object(response, 'json', mocker.Mock(return_value={}))
    assert dsctl.handle_response(response, '') is False


def test_handle_response_not_successful(mocker):
    response = mocker.Mock(spec=Response)
    mocker.patch.object(response, 'json', mocker.Mock(return_value={'success': False}))
    assert dsctl.handle_response(response, '') is False


def test_handle_response_successful(mocker):
    response = mocker.Mock(spec=Response)
    mocker.patch.object(response, 'json', mocker.Mock(return_value={'success': True}))
    assert dsctl.handle_response(response, '') is True


def test_validate_wrong_schema_type(config):
    assert dsctl.validate(config, {}, "", "abcd", False) is False


def test_validate_fail_gracefully_on_connection_error(config):
    assert dsctl.validate(config, {}, "", "event", False) is False


@responses.activate
def test_validate_meta_added_when_not_there(config):
    responses.add(
        responses.POST,
        f"{config.ds_url}/validation-requests",
        status=200,
        json={"success": True},
        match=[matchers.json_params_matcher({
            "meta": {
                "hidden": False,
                "schemaType": "event",
                "customData": {}
            },
            "data": {}
        })]
    )
    assert dsctl.validate(config, {}, "", "event", False) is True


@responses.activate
def test_validate_meta_not_added_when_there(config):
    responses.add(
        responses.POST,
        f"{config.ds_url}/validation-requests",
        status=200,
        json={"success": True},
        match=[matchers.json_params_matcher({})]
    )
    assert dsctl.validate(config, {}, "", "event", True) is True


@responses.activate
def test_promote_fails_gracefully_on_connection_error(config, deployment):
    assert dsctl.promote(
        config,
        deployment,
        "abcd",
        "message",
        False
    ) is False


@responses.activate
def test_promote_sends_the_right_body_validated_dev(config, deployment):
    responses.add(
        responses.POST,
        f"{config.ds_url}/deployment-requests",
        status=200,
        json={"success": True},
        match=[matchers.json_params_matcher({
            "name": deployment.data_structure.name,
            "vendor": deployment.data_structure.vendor,
            "format": deployment.data_structure.format,
            "version": str(deployment.version),
            "source": "VALIDATED",
            "target": "DEV",
            "message": "message"
        })]
    )

    assert dsctl.promote(
        config,
        deployment,
        "abcd",
        "message",
        to_production=False
    ) is True


@responses.activate
def test_promote_sends_the_right_body_validated_prod(config, deployment):
    responses.add(
        responses.POST,
        f"{config.ds_url}/deployment-requests",
        status=200,
        json={"success": True},
        match=[matchers.json_params_matcher({
            "name": deployment.data_structure.name,
            "vendor": deployment.data_structure.vendor,
            "format": deployment.data_structure.format,
            "version": str(deployment.version),
            "source": "DEV",
            "target": "PROD",
            "message": "message"
        })]
    )

    assert dsctl.promote(
        config,
        deployment,
        "abcd",
        "message",
        to_production=True
    ) is True


def test_resolve_resolves_correctly(data_structure, data_structure_with_meta, deployment):
    assert dsctl.resolve({}, True) is None  # Invalid input
    assert dsctl.resolve(data_structure, True) is None  # no meta but includes_meta=True
    assert dsctl.resolve(data_structure, False) == deployment  # no meta and includes_meta=False
    assert dsctl.resolve(data_structure_with_meta, True) == deployment  # with meta and includes_meta=True
    assert dsctl.resolve(data_structure_with_meta, False) is None  # with meta and includes_meta=False

    d = deepcopy(data_structure)
    del d['self']['vendor']
    assert dsctl.resolve(d, False) is None

    d = deepcopy(data_structure)
    del d['self']['name']
    assert dsctl.resolve(d, False) is None

    d = deepcopy(data_structure)
    del d['self']['format']
    assert dsctl.resolve(d, False) is None

    d = deepcopy(data_structure)
    del d['self']['version']
    assert dsctl.resolve(d, False) is None

    d = deepcopy(data_structure)
    d['self']['version'] = "incorrect"
    assert dsctl.resolve(d, False) is None


def test_filename_parsing(mocker):
    with open("f", "w") as f:
        f.write("{}")
    assert dsctl.parse_input_file("f") == {}
    os.remove("f")

    mocker.patch.object(sys, 'stdin', io.StringIO('{"a": 1}'))
    assert dsctl.parse_input_file(None) == {"a": 1}


@responses.activate
def test_main_flow_validate(mocker, args, config, token_url, data_structure, data_structure_with_meta):
    mocker.patch.object(args, 'promote_to_dev', False)
    mocker.patch.object(args, 'promote_to_prod', False)
    responses.add(responses.GET, token_url, json={"accessToken": "abcd"}, status=200)
    responses.add(
        responses.POST,
        f"{config.ds_url}/validation-requests",
        status=200,
        json={"success": True},
        match=[matchers.json_params_matcher(data_structure_with_meta)]
    )
    assert dsctl.flow(args, config) is True


@responses.activate
def test_main_flow_promote_to_dev(mocker, args, config, token_url, deployment):
    mocker.patch.object(args, 'promote_to_dev', True)
    mocker.patch.object(args, 'promote_to_prod', False)
    responses.add(responses.GET, token_url, json={"accessToken": "abcd"}, status=200)
    responses.add(
        responses.POST,
        f"{config.ds_url}/deployment-requests",
        status=200,
        json={"success": True},
        match=[matchers.json_params_matcher({
            "name": deployment.data_structure.name,
            "vendor": deployment.data_structure.vendor,
            "format": deployment.data_structure.format,
            "version": str(deployment.version),
            "source": "VALIDATED",
            "target": "DEV",
            "message": "No message provided"
        })]
    )
    assert dsctl.flow(args, config) is True


@responses.activate
def test_main_flow_promote_to_prod(mocker, args, config, token_url, deployment):
    mocker.patch.object(args, 'promote_to_dev', False)
    mocker.patch.object(args, 'promote_to_prod', True)
    responses.add(responses.GET, token_url, json={"accessToken": "abcd"}, status=200)
    responses.add(
        responses.POST,
        f"{config.ds_url}/deployment-requests",
        status=200,
        json={"success": True},
        match=[matchers.json_params_matcher({
            "name": deployment.data_structure.name,
            "vendor": deployment.data_structure.vendor,
            "format": deployment.data_structure.format,
            "version": str(deployment.version),
            "source": "DEV",
            "target": "PROD",
            "message": "No message provided"
        })]
    )
    assert dsctl.flow(args, config) is True
