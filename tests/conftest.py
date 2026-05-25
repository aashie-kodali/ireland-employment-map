"""
tests/conftest.py
=================
Shared pytest configuration for the Ireland Employment Map test suite.

Registers the 'slow' marker so pytest doesn't emit PytestUnknownMarkWarning
when test_pipeline.py marks its end-to-end tests.

Usage:
  pytest tests/ -v                   # run all tests
  pytest tests/ -v -m "not slow"     # fast tests only (no pipeline required)
  pytest tests/ -v -m slow           # end-to-end pipeline tests only
"""

import pytest


def pytest_configure(config) -> None:
    config.addinivalue_line(
        "markers",
        "slow: marks tests as slow end-to-end pipeline checks "
        "(requires 'make data' and 'make map' to have been run first)",
    )
