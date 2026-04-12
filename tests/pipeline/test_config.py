"""Tests for tributary.pipeline.config — YAML config loading with inheritance."""

import pytest
import yaml

from tributary.pipeline.config import load_config


def _write_yaml(path, data):
    """Helper: write a dict as YAML to the given path."""
    with open(path, "w") as f:
        yaml.dump(data, f)


# --- 1. Basic load without extends (plain YAML) ---

def test_basic_load_no_extends(tmp_path):
    cfg_file = tmp_path / "config.yaml"
    data = {"source": {"type": "local"}, "chunker": {"strategy": "fixed"}}
    _write_yaml(cfg_file, data)

    result = load_config(str(cfg_file))
    assert result == data


# --- 2. Extends merges base config with overrides ---

def test_extends_merges_base(tmp_path):
    base_file = tmp_path / "base.yaml"
    _write_yaml(base_file, {
        "source": {"type": "local", "params": {"directory": "/data"}},
        "chunker": {"strategy": "fixed"},
    })

    child_file = tmp_path / "child.yaml"
    _write_yaml(child_file, {
        "extends": "base.yaml",
        "source": {"type": "s3"},
    })

    result = load_config(str(child_file))
    # source should be overridden (deep-merged)
    assert result["source"]["type"] == "s3"
    # params from base should still be present (deep merge of nested dict)
    assert result["source"]["params"] == {"directory": "/data"}
    # chunker from base should be inherited
    assert result["chunker"] == {"strategy": "fixed"}


# --- 3. Deep merge — nested dicts merge, scalars override ---

def test_deep_merge_nested(tmp_path):
    base_file = tmp_path / "base.yaml"
    _write_yaml(base_file, {
        "pipeline": {
            "max_workers": 4,
            "batch_size": 256,
            "retry_policy": {"max_retries": 3, "backoff": 1.5},
        }
    })

    child_file = tmp_path / "child.yaml"
    _write_yaml(child_file, {
        "extends": "base.yaml",
        "pipeline": {
            "max_workers": 8,
            "retry_policy": {"max_retries": 5},
        }
    })

    result = load_config(str(child_file))
    assert result["pipeline"]["max_workers"] == 8          # overridden scalar
    assert result["pipeline"]["batch_size"] == 256          # inherited
    assert result["pipeline"]["retry_policy"]["max_retries"] == 5   # overridden
    assert result["pipeline"]["retry_policy"]["backoff"] == 1.5     # inherited


def test_deep_merge_list_replaced(tmp_path):
    base_file = tmp_path / "base.yaml"
    _write_yaml(base_file, {"tags": [1, 2, 3]})

    child_file = tmp_path / "child.yaml"
    _write_yaml(child_file, {"extends": "base.yaml", "tags": [4, 5]})

    result = load_config(str(child_file))
    assert result["tags"] == [4, 5]  # list replaced, not appended


# --- 4. Chain — A extends B extends C ---

def test_chain_extends(tmp_path):
    c_file = tmp_path / "c.yaml"
    _write_yaml(c_file, {"level": "c", "only_c": True, "shared": {"x": 1, "y": 2}})

    b_file = tmp_path / "b.yaml"
    _write_yaml(b_file, {"extends": "c.yaml", "level": "b", "only_b": True, "shared": {"y": 99}})

    a_file = tmp_path / "a.yaml"
    _write_yaml(a_file, {"extends": "b.yaml", "level": "a"})

    result = load_config(str(a_file))
    assert result["level"] == "a"        # from A
    assert result["only_b"] is True      # from B
    assert result["only_c"] is True      # from C
    assert result["shared"]["x"] == 1    # from C
    assert result["shared"]["y"] == 99   # from B (overrode C)


# --- 5. Circular extends raises ValueError ---

def test_circular_extends_raises(tmp_path):
    a_file = tmp_path / "a.yaml"
    b_file = tmp_path / "b.yaml"

    _write_yaml(a_file, {"extends": "b.yaml", "name": "a"})
    _write_yaml(b_file, {"extends": "a.yaml", "name": "b"})

    with pytest.raises(ValueError, match="[Cc]ircular"):
        load_config(str(a_file))


def test_self_circular_extends(tmp_path):
    a_file = tmp_path / "a.yaml"
    _write_yaml(a_file, {"extends": "a.yaml", "name": "a"})

    with pytest.raises(ValueError, match="[Cc]ircular"):
        load_config(str(a_file))


# --- 6. Relative path resolution ---

def test_relative_path_resolution(tmp_path):
    """extends path is resolved relative to the config file's directory."""
    sub = tmp_path / "sub"
    sub.mkdir()

    base_file = tmp_path / "base.yaml"
    _write_yaml(base_file, {"from_base": True})

    child_file = sub / "child.yaml"
    _write_yaml(child_file, {"extends": "../base.yaml", "from_child": True})

    result = load_config(str(child_file))
    assert result["from_base"] is True
    assert result["from_child"] is True


def test_extends_key_not_in_output(tmp_path):
    """The 'extends' key itself should not appear in the returned config."""
    base_file = tmp_path / "base.yaml"
    _write_yaml(base_file, {"key": "value"})

    child_file = tmp_path / "child.yaml"
    _write_yaml(child_file, {"extends": "base.yaml", "other": 1})

    result = load_config(str(child_file))
    assert "extends" not in result
