from rclone_api.rclone import Rclone


class Remote:
    """Remote (root) directory."""

    def __init__(self, name: str, rclone: Rclone) -> None:
        self.name = name
        self.rclone = rclone

    def __str__(self) -> str:
        return f"{self.name}:"
