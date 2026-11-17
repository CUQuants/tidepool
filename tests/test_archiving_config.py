from pathlib import Path

import pytest
import yaml

from main import create_default_config, load_config
from src.archiving_config import DEFAULT_ARCHIVING_CONFIG, validate_archiving_config


def test_partial_settings_preserve_overrides_without_mutating_defaults():
    overrides = {'delete_local': False, 'upload_delay_seconds': 0,
                 'scan_interval_seconds': 0.5, 'max_upload_attempts': 1,
                 'retry_delay_seconds': 0, 'deletion_delay_seconds': 0,
                 'state_file': '/tmp/archive.sqlite', 'scan_root': '/tmp/hot',
                 'cloud_prefix': 'research/tidepool'}
    settings = validate_archiving_config(overrides)
    assert all(settings[key] == value for key, value in overrides.items())
    assert settings['cloud_provider'] == 'stub'
    settings['scan_interval_seconds'] = 1
    assert overrides['scan_interval_seconds'] == 0.5
    assert validate_archiving_config({})['scan_interval_seconds'] == 300


@pytest.mark.parametrize('field', [
    'upload_delay_seconds', 'scan_interval_seconds',
    'retry_delay_seconds', 'deletion_delay_seconds',
])
@pytest.mark.parametrize('value', [-1, True, '60', None, float('nan'), float('inf')])
def test_invalid_timings_are_rejected(field, value):
    with pytest.raises(ValueError, match=field):
        validate_archiving_config({field: value})


@pytest.mark.parametrize('field,value', [
    ('scan_interval_seconds', 0),
    ('max_upload_attempts', 0), ('max_upload_attempts', -1),
    ('max_upload_attempts', 1.5), ('max_upload_attempts', True),
    ('max_upload_attempts', '3'),
    ('delete_local', 'false'), ('delete_local', 1),
    ('cloud_provider', 's3'),
])
def test_invalid_settings_are_rejected(field, value):
    with pytest.raises(ValueError, match=field):
        validate_archiving_config({field: value})


@pytest.mark.parametrize('field', ['state_file', 'scan_root', 'cloud_provider', 'cloud_prefix'])
@pytest.mark.parametrize('value', ['', '   ', None, 123])
def test_invalid_paths_and_cloud_strings_are_rejected(field, value):
    with pytest.raises(ValueError, match=field):
        validate_archiving_config({field: value})


def test_unknown_setting_is_rejected():
    with pytest.raises(ValueError, match='upload_delay_second'):
        validate_archiving_config({'upload_delay_second': 60})


@pytest.mark.parametrize('value', [None, [], 'stub', False])
def test_section_must_be_a_mapping(value):
    with pytest.raises(ValueError, match='archiving must be a mapping'):
        validate_archiving_config(value)


def test_old_config_gets_defaults(tmp_path):
    path = tmp_path / 'config.yaml'
    path.write_text('markets: [BTC/USD]\n')
    assert load_config(str(path))['archiving'] == DEFAULT_ARCHIVING_CONFIG


def test_yaml_overrides_are_loaded(tmp_path):
    path = tmp_path / 'config.yaml'
    path.write_text('archiving:\n  delete_local: false\n  max_upload_attempts: 5\n')
    settings = load_config(str(path))['archiving']
    assert settings['delete_local'] is False
    assert settings['max_upload_attempts'] == 5
    assert settings['upload_delay_seconds'] == 86400


@pytest.mark.parametrize('document,error', [
    ('', 'YAML mapping'),
    ('[]', 'YAML mapping'),
    ('archiving: null', 'archiving must be a mapping'),
    ('archiving:\n  delete_local: "false"', 'archiving.delete_local'),
])
def test_load_config_stops_on_invalid_settings(tmp_path, caplog, document, error):
    path = tmp_path / 'config.yaml'
    path.write_text(document)
    with pytest.raises(SystemExit) as exc:
        load_config(str(path))
    assert exc.value.code == 1
    assert error in caplog.text


def test_generated_and_example_configs_have_complete_defaults(tmp_path):
    generated = tmp_path / 'config.yaml'
    create_default_config(str(generated))
    root = Path(__file__).resolve().parents[1]
    for path in (generated, root / 'config.yaml', root / 'config.okx.example.yaml'):
        raw = yaml.safe_load(path.read_text())
        assert raw['archiving'] == DEFAULT_ARCHIVING_CONFIG
        assert load_config(str(path))['archiving'] == DEFAULT_ARCHIVING_CONFIG
