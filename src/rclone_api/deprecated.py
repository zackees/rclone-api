import functools
import warnings


def deprecated(new_func_name: str):
    """Decorator to mark functions as deprecated.

    Args:
        new_func_name: The name of the function that should be used instead.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            warnings.warn(
                f"{func.__name__}() is deprecated; use {new_func_name}() instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            return func(*args, **kwargs)

        return wrapper

    return decorator
