# core/schema_registry.py
from __future__ import annotations
from typing import Callable, List, Tuple
from sqlalchemy.engine import Engine
import pkgutil
import importlib
import sys
from pathlib import Path

# Schema installer type
SchemaInstaller = Callable[[Engine], None]

# Registry: (name, installer_func)
_REGISTRY: List[Tuple[str, SchemaInstaller]] = []

def register(
    name: str | SchemaInstaller, installer: SchemaInstaller | None = None
) -> SchemaInstaller | Callable[[SchemaInstaller], SchemaInstaller]:
    """
    Registers a schema installer function.
    Can be used as a decorator (@register) or a function call (register("name", fn)).
    """
    # Used as @register("name")
    if isinstance(name, str) and installer is None:
        def decorator(fn: SchemaInstaller) -> SchemaInstaller:
            _REGISTRY.append((name, fn))
            return fn
        return decorator
    
    # Used as @register
    elif callable(name) and installer is None:
        fn = name
        _REGISTRY.append((fn.__name__, fn))
        return fn
    
    # Used as register("name", fn)
    elif isinstance(name, str) and callable(installer):
        _REGISTRY.append((name, installer))
        return installer
    
    raise TypeError("Invalid usage of @register")

def run_all(engine: Engine):
    """
    Runs all registered schema installers in order.
    """
    print(f"SchemaRegistry: Running {len(_REGISTRY)} installers...")
    for name, installer_fn in _REGISTRY:
        try:
            print(f"  -> Applying schema: {name}")
            installer_fn(engine)
        except Exception as e:
            print(f"  -> FAILED to apply schema {name}: {e}")
            import traceback
            traceback.print_exc()
            # Continue with other installers instead of crashing
    print("SchemaRegistry: All installers complete.")

def _REGISTRY_count() -> int:
    return len(_REGISTRY)

def auto_discover(
    start_path: str | Path = "schemas", 
    root_package: str | None = None
):
    """
    Dynamically imports all modules in a directory to trigger @register decorators.
    
    :param start_path: The directory path to start discovery (e.g., "schemas").
    :param root_package: The parent package name (optional).
    """
    if isinstance(start_path, str):
        start_path = Path(start_path)

    if not start_path.is_dir():
        print(f"Schema auto_discover: Path {start_path} is not a directory. Skipping.")
        return

    # If root_package is provided, form the import name (e.g., "core.schemas")
    # If not, we need to add the parent of start_path to sys.path
    if root_package:
        base_import_name = f"{root_package}.{start_path.name}"
    else:
        # Add parent dir to sys.path if not already there
        parent_dir = str(start_path.parent.resolve())
        if parent_dir not in sys.path:
            sys.path.insert(0, parent_dir)
        base_import_name = start_path.name

    print(f"Schema auto_discover: Discovering modules in {start_path} (base: {base_import_name})...")
    
    for _, module_name, is_pkg in pkgutil.walk_packages(
        path=[str(start_path)], 
        prefix=f"{base_import_name}."
    ):
        if is_pkg:
            continue  # Don't import packages themselves
        
        # Skip certain modules if needed (e.g., old migration files)
        skip_modules = [
            f"{base_import_name}.faculty_schema_migration",  # Old monolithic migration - skip it
        ]
        
        if module_name in skip_modules:
            print(f"  -> Skipping (deprecated): {module_name}")
            continue
            
        try:
            importlib.import_module(module_name)
            print(f"  -> Discovered: {module_name}")
        except Exception as e:
            print(f"  -> FAILED to import module {module_name}: {e}")
            import traceback
            traceback.print_exc()
