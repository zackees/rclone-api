from typing import Any


class Remote:
    """Remote (root) directory."""

    def __init__(self, name: str, rclone: Any) -> None:
        from rclone_api.rclone import Rclone

        if ":" in name:
            raise ValueError("Remote name cannot contain ':'")

        assert isinstance(rclone, Rclone)
        self.name = name
        self.rclone: Rclone = rclone

    def __str__(self) -> str:
        return f"{self.name}:"
