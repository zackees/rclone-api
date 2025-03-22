import abc
import shutil
from pathlib import Path


class FileSystem(abc.ABC):
    def __init__(self) -> None:
        pass

    @abc.abstractmethod
    def copy(self, src: Path | str, dest: Path | str) -> None:
        pass

    @abc.abstractmethod
    def read_binary(self, path: Path | str) -> bytes:
        pass

    @abc.abstractmethod
    def exists(self, path: Path | str) -> bool:
        pass

    @abc.abstractmethod
    def write_binary(self, path: Path | str, data: bytes) -> None:
        pass

    @abc.abstractmethod
    def mkdir(self, path: str, parents=True, exist_ok=True) -> None:
        pass

    @abc.abstractmethod
    def get_path(self, path: str) -> "FSPath":
        pass

    def read_text(self, path: Path | str) -> str:
        utf = self.read_binary(path)
        return utf.decode("utf-8")

    def write_text(self, path: Path | str, data: str, encoding: str | None) -> None:
        encoding = encoding or "utf-8"
        utf = data.encode(encoding)
        self.write_binary(path, utf)


class RealFileSystem(FileSystem):

    @staticmethod
    def get_real_path(path: Path | str) -> "FSPath":
        path_str = Path(path).as_posix()
        return FSPath(RealFileSystem(), path_str)

    def __init__(self) -> None:
        super().__init__()

    def copy(self, src: Path | str, dest: Path | str) -> None:
        shutil.copy(str(src), str(dest))

    def read_binary(self, path: Path | str) -> bytes:
        with open(path, "rb") as f:
            return f.read()

    def write_binary(self, path: Path | str, data: bytes) -> None:
        with open(path, "wb") as f:
            f.write(data)

    def exists(self, path: Path | str) -> bool:
        return Path(path).exists()

    def mkdir(self, path: str, parents=True, exist_ok=True) -> None:
        Path(path).mkdir(parents=parents, exist_ok=exist_ok)

    def get_path(self, path: str) -> "FSPath":
        return FSPath(self, path)


class RemoteFileSystem(FileSystem):
    def __init__(self, rclone_conf: Path, src: str) -> None:
        from rclone_api import HttpServer, Rclone

        super().__init__()
        self.rclone_conf = rclone_conf
        self.rclone: Rclone = Rclone(rclone_conf)
        self.server: HttpServer = self.rclone.serve_http(src=src)
        self.shutdown = False

    def _to_str(self, path: Path | str) -> str:
        if isinstance(path, Path):
            return path.as_posix()
        return path

    def copy(self, src: Path | str, dest: Path | str) -> None:
        src = self._to_str(src)
        dest = self._to_str(dest)
        self.rclone.copy(src, dest)

    def read_binary(self, path: Path | str) -> bytes:
        path = self._to_str(path)
        err = self.rclone.read_bytes(path)
        if isinstance(err, Exception):
            raise FileNotFoundError(f"File not found: {path}")
        return err

    def write_binary(self, path: Path | str, data: bytes) -> None:
        path = self._to_str(path)
        self.rclone.write_bytes(data, path)

    def exists(self, path: Path | str) -> bool:
        path = self._to_str(path)
        return self.server.exists(path)

    def mkdir(self, path: str, parents=True, exist_ok=True) -> None:
        raise NotImplementedError("RemoteFileSystem does not support mkdir")

    def get_path(self, path: str) -> "FSPath":
        return FSPath(self, path)

    def dispose(self) -> None:
        if self.shutdown:
            return
        self.shutdown = True
        self.server.shutdown()

    def __del__(self) -> None:
        self.dispose()


class FSPath:
    def __init__(self, fs: FileSystem, path: str) -> None:
        self.fs = fs
        self.path = path

    def read_text(self) -> str:
        return self.fs.read_text(self.path)

    def read_binary(self) -> bytes:
        return self.fs.read_binary(self.path)

    def exists(self) -> bool:
        return self.fs.exists(self.path)

    def __str__(self) -> str:
        return self.path

    def __repr__(self) -> str:
        return f"FSPath({self.path})"

    def mkdir(self, parents=True, exist_ok=True) -> None:
        self.fs.mkdir(self.path, parents=parents, exist_ok=exist_ok)

    def write_text(self, data: str, encoding: str | None = None) -> None:
        self.fs.write_text(self.path, data, encoding=encoding)

    def rmtree(self, ignore_errors=False) -> None:
        assert self.exists(), f"Path does not exist: {self.path}"
        # check fs is RealFileSystem
        assert isinstance(self.fs, RealFileSystem)
        shutil.rmtree(self.path, ignore_errors=ignore_errors)

    @property
    def name(self) -> str:
        return Path(self.path).name

    @property
    def parent(self) -> "FSPath":
        parent_path = Path(self.path).parent
        parent_str = parent_path.as_posix()
        return FSPath(self.fs, parent_str)

    def __truediv__(self, other: str) -> "FSPath":
        new_path = Path(self.path) / other
        return FSPath(self.fs, new_path.as_posix())

    # hashable
    def __hash__(self) -> int:
        return hash(f"{repr(self.fs)}:{self.path}")
