"""Configuration loading with inheritance support for Tributary pipelines.

Supports YAML config files with:
  * ``extends`` — point to a base config file (path relative to the current
    config's directory). Base configs are loaded first and then deep-merged
    with the child config. Chaining (A extends B extends C) is supported.
    Circular references raise ValueError.
  * ``${VAR}`` and ``${VAR:-default}`` env-var substitution in string values.
    Applied after deep-merge, so base and child configs can both reference
    environment variables.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml


_ENV_VAR_PATTERN = re.compile(
    r"""
    \$\{                    # opening ${
        ([A-Za-z_][A-Za-z0-9_]*)   # VAR name
        (?:                 # optional :-default
            :-
            ([^}]*)         # default value (anything except closing brace)
        )?
    \}                      # closing }
    """,
    re.VERBOSE,
)


def _substitute_env_vars(value: Any) -> Any:
    """Recursively replace ``${VAR}`` / ``${VAR:-default}`` in string values.

    Dicts and lists are walked; scalars other than strings are returned as-is.
    If a referenced env var is unset and no default is provided, the literal
    ``${VAR}`` is left in place so validation can catch it — we don't raise
    so that schema errors surface with the path to the offending field.
    """
    if isinstance(value, dict):
        return {k: _substitute_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_substitute_env_vars(v) for v in value]
    if isinstance(value, str):
        def replace(match: re.Match[str]) -> str:
            name, default = match.group(1), match.group(2)
            env_value = os.environ.get(name)
            if env_value is not None:
                return env_value
            if default is not None:
                return default
            return match.group(0)  # leave ${VAR} untouched
        return _ENV_VAR_PATTERN.sub(replace, value)
    return value


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge *override* into *base*.

    * Dicts are merged recursively (child values win).
    * Lists and scalars in *override* replace the base value entirely.

    Returns a **new** dict — neither input is mutated.
    """
    merged: dict[str, Any] = dict(base)
    for key, value in override.items():
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(value, dict)
        ):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(path: str) -> dict:
    """Load a YAML config file with optional inheritance and env-var expansion.

    Parameters
    ----------
    path:
        Filesystem path to the YAML config file.

    Returns
    -------
    dict
        The fully-resolved configuration dictionary with ``${VAR}``
        references replaced by their environment-variable values.

    Raises
    ------
    ValueError
        If a circular ``extends`` chain is detected.
    """
    cfg = _load_config_impl(path, seen=set())
    return _substitute_env_vars(cfg)


def _load_config_impl(path: str, seen: set[str]) -> dict:
    resolved = str(Path(path).resolve())

    if resolved in seen:
        raise ValueError(
            f"Circular extends detected: {resolved} already in chain"
        )
    seen.add(resolved)

    with open(resolved) as f:
        cfg: dict = yaml.safe_load(f) or {}

    extends = cfg.pop("extends", None)

    if extends is not None:
        base_path = str(Path(resolved).parent / extends)
        base_cfg = _load_config_impl(base_path, seen)
        cfg = _deep_merge(base_cfg, cfg)

    return cfg
