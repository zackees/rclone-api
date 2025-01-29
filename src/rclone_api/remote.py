from typing import Any


class Remote:
    """Remote (root) directory."""

    def __init__(self, name: str, rclone: Any) -> None:
        from rclone_api.rclone import Rclone

        assert isinstance(rclone, Rclone)
        self.name = name
        self.rclone: Rclone = rclone

    def __str__(self) -> str:
        return f"{self.name}:"
