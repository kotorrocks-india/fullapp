# action_registry.py

from typing import Dict, Callable

_action_handlers: Dict[str, Callable] = {}


def register_action_handler(object_type: str, action: str):
    """
    Decorator to register action handlers for specific object_type.action combinations.
    Example:
        @register_action_handler("faculty", "delete")
        def handle_faculty_delete(conn, object_id, payload): ...
    """
    def decorator(func: Callable):
        key = f"{object_type}.{action}"
        _action_handlers[key] = func
        return func
    return decorator


def get_action_handler(object_type: str, action: str) -> Callable:
    """
    Get the appropriate handler for an object_type.action combination.
    Raises if none is registered.
    """
    key = f"{object_type}.{action}"
    handler = _action_handlers.get(key)
    if not handler:
        raise ValueError(f"No handler registered for {object_type}.{action}")
    return handler


def list_registered_handlers() -> list:
    """List all registered handlers for debugging."""
    return list(_action_handlers.keys())
