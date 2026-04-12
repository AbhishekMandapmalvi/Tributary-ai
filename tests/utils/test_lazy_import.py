import pytest
from unittest.mock import patch
from tributary.utils.lazy_import import lazy_import, _IMPORT_TO_PIP


def test_imports_installed_module():
    result = lazy_import("json")
    import json
    assert result is json


def test_raises_for_missing_module_non_interactive():
    with patch("tributary.utils.lazy_import._auto_install", False):
        with patch("sys.stdin", None):
            with pytest.raises(ImportError, match="Missing dependency"):
                lazy_import("nonexistent_package_xyz")


def test_error_message_includes_pip_command():
    with patch("tributary.utils.lazy_import._auto_install", False):
        with patch("sys.stdin", None):
            with pytest.raises(ImportError, match="pip install nonexistent_package_xyz"):
                lazy_import("nonexistent_package_xyz")


def test_uses_pip_name_mapping():
    with patch("tributary.utils.lazy_import._auto_install", False):
        with patch("sys.stdin", None):
            with pytest.raises(ImportError, match="pip install pymupdf"):
                lazy_import("fitz_nonexistent", pip_name="pymupdf")


def test_import_to_pip_has_common_mappings():
    assert "fitz" in _IMPORT_TO_PIP
    assert "bs4" in _IMPORT_TO_PIP
    assert "yaml" in _IMPORT_TO_PIP
    assert "qdrant_client" in _IMPORT_TO_PIP
