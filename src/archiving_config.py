"""Shared archiving defaults and validation for collectors and the future sweeper."""

import math


DEFAULT_ARCHIVING_CONFIG = {
    'upload_delay_seconds': 86400,
    'scan_interval_seconds': 300,
    'max_upload_attempts': 3,
    'retry_delay_seconds': 60,
    'delete_local': True,
    'deletion_delay_seconds': 86400,
    'state_file': 'data/archive_state.sqlite',
    'scan_root': 'data/hot',
    'cloud_provider': 'stub',
    'cloud_prefix': 'tidepool',
}


def validate_archiving_config(values: dict) -> dict:
    """Return a fresh settings dict with defaults filled in, or raise ValueError.

    Timings are seconds. Relative paths follow the collector's convention:
    they are relative to the working directory, not the YAML file.
    Validation does not create files or directories.
    """
    if not isinstance(values, dict):
        raise ValueError('archiving must be a mapping of settings')
    unknown = values.keys() - DEFAULT_ARCHIVING_CONFIG.keys()
    if unknown:
        raise ValueError(f'Unknown archiving settings: {", ".join(sorted(map(str, unknown)))}')

    settings = {**DEFAULT_ARCHIVING_CONFIG, **values}
    for name in ('upload_delay_seconds', 'scan_interval_seconds',
                 'retry_delay_seconds', 'deletion_delay_seconds'):
        value = settings[name]
        if type(value) not in (int, float) or (isinstance(value, float) and not math.isfinite(value)):
            raise ValueError(f'archiving.{name} must be a finite number of seconds')
        if value < 0 or (name == 'scan_interval_seconds' and value == 0):
            bound = 'greater than zero' if name == 'scan_interval_seconds' else 'zero or greater'
            raise ValueError(f'archiving.{name} must be {bound}')

    attempts = settings['max_upload_attempts']
    if type(attempts) is not int or attempts < 1:
        raise ValueError('archiving.max_upload_attempts must be an integer of at least 1')
    if type(settings['delete_local']) is not bool:
        raise ValueError('archiving.delete_local must be a boolean (true or false)')

    for name in ('state_file', 'scan_root', 'cloud_provider', 'cloud_prefix'):
        if not isinstance(settings[name], str) or not settings[name].strip():
            raise ValueError(f'archiving.{name} must be a non-empty string')
    if settings['cloud_provider'] != 'stub':
        raise ValueError("archiving.cloud_provider must be 'stub' until a provider is implemented")

    return settings
