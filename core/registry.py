"""
core/registry.py
------------------
The central registration engine. This file should NEVER need to change
as you add new data sources, indicators, or scenarios.

How it works:
  Any file in data_sources/, indicators/, or scenarios/ uses a decorator
  (@register_data_source, @register_indicator, @register_scenario) to
  add itself into the relevant dictionary below. main.py then looks up
  components by name from these dictionaries — it never imports them
  directly.
"""

DATA_SOURCE_REGISTRY = {}
INDICATOR_REGISTRY = {}
SCENARIO_REGISTRY = {}


def register_data_source(name):
    def wrapper(func):
        if name in DATA_SOURCE_REGISTRY:
            raise ValueError(f"Data source '{name}' already registered.")
        DATA_SOURCE_REGISTRY[name] = func
        return func
    return wrapper


def register_indicator(name):
    def wrapper(func):
        if name in INDICATOR_REGISTRY:
            raise ValueError(f"Indicator '{name}' already registered.")
        INDICATOR_REGISTRY[name] = func
        return func
    return wrapper


def register_scenario(name):
    def wrapper(func):
        if name in SCENARIO_REGISTRY:
            raise ValueError(f"Scenario '{name}' already registered.")
        SCENARIO_REGISTRY[name] = func
        return func
    return wrapper


def list_registered():
    """Utility to see what's currently available — useful for debugging/control panel."""
    return {
        "data_sources": list(DATA_SOURCE_REGISTRY.keys()),
        "indicators": list(INDICATOR_REGISTRY.keys()),
        "scenarios": list(SCENARIO_REGISTRY.keys()),
    }
