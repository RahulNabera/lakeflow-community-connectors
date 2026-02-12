"""Regression tests for merge_python_source.py import handling."""
import pytest
from merge_python_source import deduplicate_imports


def test_inline_pylint_comment_single_line():
    """# pylint: disable on single-line import must not corrupt names."""
    imports = [["from azure.servicebus import ServiceBusClient, ServiceBusSubQueue  # pylint: disable=import-error"]]
    result = deduplicate_imports(imports)
    merged = "\n".join(result)
    assert "# pylint" not in merged
    assert "ServiceBusClient" in merged
    assert "ServiceBusSubQueue" in merged


def test_inline_pylint_comment_multiline():
    """# pylint: disable on opening paren line must not corrupt names."""
    imports = [["from azure.identity import (  # pylint: disable=import-error\n    DefaultAzureCredential,\n    ClientSecretCredential,\n)"]]
    result = deduplicate_imports(imports)
    merged = "\n".join(result)
    assert "# pylint" not in merged
    assert "DefaultAzureCredential" in merged
    assert "ClientSecretCredential" in merged


def test_no_comment_imports_unchanged():
    """Imports without comments should be unaffected."""
    imports = [["from datetime import datetime", "import json"]]
    result = deduplicate_imports(imports)
    assert "from datetime import datetime" in result
    assert "import json" in result
