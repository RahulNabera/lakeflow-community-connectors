"""
Pytest fixtures and configuration for Azure Service Bus stress tests.

Markers:
    premium  - Tests requiring Azure Service Bus Premium tier.

Usage:
    # Run all tests
    pytest sources/azure_servicebus/test/stress_test.py -v

    # Run only premium-tier tests
    pytest sources/azure_servicebus/test/stress_test.py -v -m premium

    # Exclude premium-tier tests
    pytest sources/azure_servicebus/test/stress_test.py -v -m "not premium"
"""

import os
import pytest
from .stress_test_utils import (
    get_connection_string,
    StressTestResources,
    CONNECTION_STRING_ENV,
)


# ---------------------------------------------------------------------------
# Pytest configuration
# ---------------------------------------------------------------------------

def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "premium: mark test as requiring Azure Service Bus Premium tier "
        "(large messages, sessions, higher throughput)",
    )


def pytest_addoption(parser):
    """Register custom command-line options."""
    parser.addoption(
        "--no-cleanup",
        action="store_true",
        default=False,
        help="Do not clean up Azure resources after tests (for debugging)",
    )
    parser.addoption(
        "--connection-string",
        action="store",
        default=None,
        help="Azure Service Bus connection string (overrides env var)",
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def connection_string(request):
    """Get the connection string from CLI arg or environment."""
    cs = request.config.getoption("--connection-string")
    if cs:
        return cs
    cs = os.environ.get(CONNECTION_STRING_ENV)
    if not cs:
        pytest.skip(
            f"No connection string. Set {CONNECTION_STRING_ENV} env var "
            "or pass --connection-string."
        )
    return cs


@pytest.fixture(scope="session")
def no_cleanup(request):
    """Whether to skip resource cleanup."""
    return request.config.getoption("--no-cleanup")


@pytest.fixture(scope="function")
def resources(connection_string, no_cleanup):
    """Provide a StressTestResources manager that auto-cleans."""
    res = StressTestResources(connection_string)
    yield res
    if not no_cleanup:
        res.cleanup()
    res.close()


@pytest.fixture(scope="session")
def session_resources(connection_string, no_cleanup):
    """Session-scoped resources (shared across all tests in file)."""
    res = StressTestResources(connection_string)
    yield res
    if not no_cleanup:
        res.cleanup()
    res.close()
