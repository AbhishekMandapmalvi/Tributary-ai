"""Lazy dependency loader with automatic installation.

When an optional dependency is missing, prompts the user to install it
(or auto-installs if TRIBUTARY_AUTO_INSTALL=1 is set).

Usage:
    openai = lazy_import("openai", pip_name="openai")
    client = openai.AsyncOpenAI()
"""
import importlib
import subprocess
import sys
import os
import structlog

logger = structlog.get_logger(__name__)

# Map import names to pip package names when they differ
_IMPORT_TO_PIP = {
    "fitz": "pymupdf",
    "bs4": "beautifulsoup4",
    "yaml": "pyyaml",
    "mdit_plain": "mdit-plain",
    "markdown_it": "markdown-it-py",
    "gcloud": "gcloud-aio-storage",
    "qdrant_client": "qdrant-client",
    "azure.storage.blob": "azure-storage-blob",
    "google.cloud.aiplatform": "google-cloud-aiplatform",
    "vertexai": "google-cloud-aiplatform",
    "voyageai": "voyageai",
    "aio_pika": "aio-pika",
    "aioboto3": "aioboto3",
    "gcloud.aio.pubsub": "gcloud-aio-pubsub",
    "azure.servicebus": "azure-servicebus",
    "azure.servicebus.aio": "azure-servicebus",
    "aiokafka": "aiokafka",
}

_auto_install = os.environ.get("TRIBUTARY_AUTO_INSTALL", "").lower() in ("1", "true", "yes")


def lazy_import(module_name: str, pip_name: str | None = None):
    """Import a module, auto-installing if missing.

    Args:
        module_name: The Python import name (e.g., "openai").
        pip_name: The pip package name if different from module_name.

    Returns:
        The imported module.

    Raises:
        ImportError: If the package can't be installed or user declines.
    """
    try:
        return importlib.import_module(module_name)
    except ImportError:
        pass

    package = pip_name or _IMPORT_TO_PIP.get(module_name, module_name)

    if _auto_install:
        return _install_and_import(module_name, package)

    # Interactive prompt
    if sys.stdin and sys.stdin.isatty():
        print(f"\n  Tributary needs '{package}' for this feature.")
        answer = input(f"  Install it now? (pip install {package}) [Y/n] ").strip().lower()
        if answer in ("", "y", "yes"):
            return _install_and_import(module_name, package)

    raise ImportError(
        f"Missing dependency: '{package}'. "
        f"Install with: pip install {package}\n"
        f"Or set TRIBUTARY_AUTO_INSTALL=1 to auto-install missing dependencies."
    )


def _install_and_import(module_name: str, package: str):
    """Install a package and import it."""
    logger.info("Installing dependency", package=package)
    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", package],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as e:
        raise ImportError(f"Failed to install '{package}': {e}") from e

    return importlib.import_module(module_name)
