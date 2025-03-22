import abc
import shutil
from pathlib import Path

from rclone_api.config import Config


class FS(abc.ABC):
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
    def ls(self, path: Path | str) -> list[str]:
        pass

    @abc.abstractmethod
    def cwd(self) -> "FSPath":
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


class RealFS(FS):

    @staticmethod
    def from_path(path: Path | str) -> "FSPath":
        path_str = Path(path).as_posix()
        return FSPath(RealFS(), path_str)

    def __init__(self) -> None:
        super().__init__()

    def ls(self, path: Path | str) -> list[str]:
        return [str(p) for p in Path(path).iterdir()]

    def cwd(self) -> "FSPath":
        return RealFS.from_path(Path.cwd())

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


class RemoteFS(FS):

    @staticmethod
    def from_rclone_config(
        src: str, rclone_conf: Path | Config | str | None
    ) -> "RemoteFS":
        if isinstance(rclone_conf, str):
            rclone_conf = Config(text=rclone_conf)
        if rclone_conf is None:
            curr_dir = Path.cwd() / "rclone.conf"
            if curr_dir.exists():
                rclone_conf = curr_dir
            else:
                raise ValueError("rclone_conf not found")
        return RemoteFS(rclone_conf, src)

    def __init__(self, rclone_conf: Path | Config, src: str) -> None:
        from rclone_api import HttpServer, Rclone

        super().__init__()
        self.src = src
        self.rclone_conf = rclone_conf
        self.rclone: Rclone = Rclone(rclone_conf)
        self.server: HttpServer = self.rclone.serve_http(src=src)
        self.shutdown = False

    def root(self) -> "FSPath":
        return FSPath(self, self.src)

    def cwd(self) -> "FSPath":
        return self.root()

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
        raise NotImplementedError("RemoteFS does not support mkdir")

    def is_dir(self, path: Path | str) -> bool:
        path = self._to_str(path)
        err = self.server.list(path)
        return isinstance(err, list)

    def is_file(self, path: Path | str) -> bool:
        path = self._to_str(path)
        err = self.server.list(path)
        # Make faster.
        return isinstance(err, Exception) and self.exists(path)

    def ls(self, path: Path | str) -> list[str]:
        path = self._to_str(path)
        err = self.server.list(path)
        if isinstance(err, Exception):
            raise FileNotFoundError(f"File not found: {path}, because of {err}")
        return err

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
    def __init__(self, fs: FS, path: str) -> None:
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
        # check fs is RealFS
        assert isinstance(self.fs, RealFS)
        shutil.rmtree(self.path, ignore_errors=ignore_errors)

    def ls(self) -> "list[FSPath]":
        names: list[str] = self.fs.ls(self.path)
        return [self / name for name in names]

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
