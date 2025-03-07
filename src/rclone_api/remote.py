from typing import Any


class Remote:
    """Remote (root) directory."""

    def __init__(self, name: str, rclone: Any) -> None:
        from rclone_api.rclone_impl import RcloneImpl

        if ":" in name:
            raise ValueError("Remote name cannot contain ':'")

        assert isinstance(rclone, RcloneImpl)
        self.name = name
        self.rclone: RcloneImpl = rclone

    def __str__(self) -> str:
        return f"{self.name}:"

    def __repr__(self) -> str:
        return f"Remote({self.name!r})"
