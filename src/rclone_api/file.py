from rclone_api.rpath import RPath


class File:
    """Remote file dataclass."""

    def __init__(
        self,
        path: RPath,
    ) -> None:
        self.path = path

    def read_text(self) -> str:
        """Read the file contents as bytes.

        Returns:
            bytes: The file contents

        Raises:
            RuntimeError: If no rclone instance is associated with this file
            RuntimeError: If the path represents a directory
        """
        if self.path.rclone is None:
            raise RuntimeError("No rclone instance associated with this file")
        if self.path.is_dir:
            raise RuntimeError("Cannot read a directory as bytes")

        result = self.path.rclone._run(["cat", self.path.path])
        return result.stdout

    def to_json(self) -> dict:
        """Convert the File to a JSON serializable dictionary."""
        return self.path.to_json()

    def __str__(self) -> str:
        return str(self.path)
