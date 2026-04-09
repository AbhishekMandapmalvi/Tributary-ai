"""Configuration loading with inheritance support for Tributary pipelines.

Supports YAML config files with an optional `extends` key that points to a
base config file (path relative to the current config's directory).  Base
configs are loaded first and then deep-merged with the child config.  Chaining
is supported (A extends B extends C).  Circular references raise ValueError.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


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
    """Load a YAML config file with optional inheritance via ``extends``.

    Parameters
    ----------
    path:
        Filesystem path to the YAML config file.

    Returns
    -------
    dict
        The fully-resolved configuration dictionary.

    Raises
    ------
    ValueError
        If a circular ``extends`` chain is detected.
    """
    return _load_config_impl(path, seen=set())


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
