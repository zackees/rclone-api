class Remote:
    """Remote dataclass."""

    def __init__(self, name: str) -> None:
        self.name = name

    def __str__(self) -> str:
        return f"{self.name}:"
