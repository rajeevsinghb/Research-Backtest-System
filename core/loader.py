"""
core/loader.py
----------------
Auto-discovers and imports every .py file inside data_sources/, indicators/,
and scenarios/ folders. Because each of those files registers itself via the
decorators in registry.py just by being imported, this loader is the reason
you NEVER have to manually write "from indicators.rsi import ..." anywhere.

To add a new indicator/scenario/data source: just drop a new .py file in the
right folder. To remove one: delete the file. Nothing else changes.
"""

import importlib
import pkgutil


def load_all(package_name: str):
    """Import every module inside the given package (folder) so its
    @register_* decorators run and populate the registry."""
    package = importlib.import_module(package_name)
    for _, module_name, _ in pkgutil.iter_modules(package.__path__):
        importlib.import_module(f"{package_name}.{module_name}")


def load_everything():
    load_all("data_sources")
    load_all("indicators")
    load_all("scenarios")
