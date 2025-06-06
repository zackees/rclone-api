import abc
import logging
import shutil
import warnings
from pathlib import Path
from typing import Generator

from rclone_api.config import Config
from rclone_api.fs.walk_threaded_walker import FSWalker

logger = logging.getLogger(__name__)


class FS(abc.ABC):
    def __init__(self) -> None:
        pass

    @abc.abstractmethod
    def copy(self, src: Path | str, dst: Path | str) -> None:
        pass

    @abc.abstractmethod
    def read_bytes(self, path: Path | str) -> bytes:
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
    def ls(self, path: Path | str) -> tuple[list[str], list[str]]:
        """First is files and second is directories."""
        pass

    @abc.abstractmethod
    def remove(self, path: Path | str) -> Exception | None:
        """Remove a file or symbolic link."""
        pass

    @abc.abstractmethod
    def unlink(self, path: Path | str) -> Exception | None:
        """Remove a file or symbolic link."""
        pass

    @abc.abstractmethod
    def cwd(self) -> "FSPath":
        pass

    @abc.abstractmethod
    def get_path(self, path: str) -> "FSPath":
        pass

    @abc.abstractmethod
    def dispose(self) -> None:
        pass


class RealFS(FS):

    @staticmethod
    def from_path(path: Path | str) -> "FSPath":
        path_str = Path(path).as_posix()
        return FSPath(RealFS(), path_str)

    def __init__(self) -> None:
        super().__init__()

    def ls(self, path: Path | str) -> tuple[list[str], list[str]]:
        files_and_dirs = [str(p) for p in Path(path).iterdir()]
        files = [f for f in files_and_dirs if Path(f).is_file()]
        dirs = [d for d in files_and_dirs if Path(d).is_dir()]
        return files, dirs

    def cwd(self) -> "FSPath":
        return RealFS.from_path(Path.cwd())

    def copy(self, src: Path | str, dst: Path | str) -> None:
        shutil.copy(str(src), str(dst))

    def read_bytes(self, path: Path | str) -> bytes:
        with open(path, "rb") as f:
            return f.read()

    def write_binary(self, path: Path | str, data: bytes) -> None:
        with open(path, "wb") as f:
            f.write(data)

    def exists(self, path: Path | str) -> bool:
        return Path(path).exists()

    def unlink(self, path: Path | str) -> Exception | None:
        """Remove a file or symbolic link."""
        try:
            Path(path).unlink()
            return None
        except KeyboardInterrupt:
            raise
        except FileNotFoundError as e:
            return e

    def remove(self, path: Path | str, ignore_errors=False) -> Exception | None:
        """Remove a file or directory."""
        try:
            path = Path(path)
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=ignore_errors)
            else:
                path.unlink()
            return None
        except KeyboardInterrupt:
            raise
        except Exception as e:
            return e

    def mkdir(self, path: str, parents=True, exist_ok=True) -> None:
        Path(path).mkdir(parents=parents, exist_ok=exist_ok)

    def get_path(self, path: str) -> "FSPath":
        return FSPath(self, path)

    def dispose(self) -> None:
        pass


class RemoteFS(FS):

    @staticmethod
    def from_rclone_config(
        src: str, rclone_conf: Path | Config | str | dict | None
    ) -> "RemoteFS":
        if isinstance(rclone_conf, str) or isinstance(rclone_conf, dict):
            rclone_conf = Config(text=rclone_conf)
        return RemoteFS(rclone_conf, src)

    def __init__(self, rclone_conf: Path | Config | None, src: str) -> None:
        from rclone_api import HttpServer, Rclone

        super().__init__()
        self.src = src
        self.shutdown = False
        self.server: HttpServer | None = None
        if rclone_conf is None:
            from rclone_api.config import find_conf_file

            rclone_conf = find_conf_file()
            if rclone_conf is None:
                raise FileNotFoundError("rclone.conf not found")
        self.rclone_conf = rclone_conf
        self.rclone: Rclone = Rclone(rclone_conf)
        self.server = self.rclone.serve_http(src=src)

    def root(self) -> "FSPath":
        return FSPath(self, self.src)

    def cwd(self) -> "FSPath":
        return self.root()

    def _to_str(self, path: Path | str) -> str:
        if isinstance(path, Path):
            return path.as_posix()
        return path

    def _to_remote_path(self, path: str | Path) -> str:
        return Path(path).relative_to(self.src).as_posix()

    def copy(self, src: Path | str, dst: Path | str) -> None:
        from rclone_api.completed_process import CompletedProcess

        src = src if isinstance(src, Path) else Path(src)
        # if not src.is_file():
        #     raise FileNotFoundError(f"File not found: {src}")
        dst_remote_path = self._to_remote_path(dst)

        is_s3 = self.rclone.is_s3(dst_remote_path)
        if is_s3:
            filesize = src.stat().st_size
            if filesize < 1024 * 1024 * 1024:  # 1GB
                logger.info(f"S3 OPTIMIZED: Copying {src} -> {dst_remote_path}")
                err = self.rclone.copy_file_s3(src, dst_remote_path)
                if isinstance(err, Exception):
                    raise FileNotFoundError(
                        f"File not found: {src}, specified by {err}"
                    )
                return
        # Fallback.
        logging.info(f"Copying {src} -> {dst}")
        src_path = src.as_posix()
        dst = dst if isinstance(dst, str) else dst.as_posix()
        cp: CompletedProcess = self.rclone.copy_to(src_path, dst)
        if cp.returncode != 0:
            raise FileNotFoundError(f"File not found: {src}, specified by {cp.stderr}")

    def read_bytes(self, path: Path | str) -> bytes:
        path = self._to_str(path)
        err = self.rclone.read_bytes(path)
        if isinstance(err, Exception):
            raise FileNotFoundError(f"File not found: {path}")
        return err

    def write_binary(self, path: Path | str, data: bytes) -> None:
        path = self._to_str(path)
        self.rclone.write_bytes(data, path)  # Already optimized for s3.

    def exists(self, path: Path | str) -> bool:
        from rclone_api.http_server import HttpServer

        assert isinstance(self.server, HttpServer)
        path = self._to_str(path)
        dst_rel = self._to_remote_path(path)
        return self.server.exists(dst_rel)

    def mkdir(self, path: str, parents=True, exist_ok=True) -> None:
        # Ignore mkdir for remote backend, it will be made when file is written.
        import warnings

        warnings.warn("mkdir is not supported for remote backend", stacklevel=2)
        return None

    def is_dir(self, path: Path | str) -> bool:
        from rclone_api.http_server import HttpServer

        assert isinstance(self.server, HttpServer)
        path = self._to_remote_path(path)
        err = self.server.list(path)
        return isinstance(err, list)

    def is_file(self, path: Path | str) -> bool:
        from rclone_api.http_server import HttpServer

        assert isinstance(self.server, HttpServer)
        path = self._to_remote_path(path)
        err = self.server.list(path)
        # Make faster.
        return isinstance(err, Exception) and self.exists(path)

    def ls(self, path: Path | str) -> tuple[list[str], list[str]]:
        from rclone_api.http_server import HttpServer

        assert isinstance(self.server, HttpServer)
        path = self._to_remote_path(path)
        err = self.server.list(path)
        if isinstance(err, Exception):
            raise FileNotFoundError(f"File not found: {path}, because of {err}")
        return err

    def unlink(self, path: Path | str) -> Exception | None:
        return self.remove(path)

    def remove(self, path: Path | str) -> Exception | None:
        """Remove a file or symbolic link."""

        # remove this
        # path = self._to_remote_path(path)
        path = path if isinstance(path, str) else path.as_posix()
        err = self.rclone.delete_files(path)
        if isinstance(err, Exception):
            return FileNotFoundError(f"File not found: {path}, because of {err}")
        if isinstance(path, Path):
            path = path.as_posix()
        # assert isinstance(self.server, HttpServer)
        # self.server.delete(path)
        return None

    def get_path(self, path: str) -> "FSPath":
        return FSPath(self, path)

    def dispose(self) -> None:
        if self.shutdown or not self.server:
            return
        self.shutdown = True
        self.server.shutdown()

    def __del__(self) -> None:
        self.dispose()


class FSPath:

    @staticmethod
    def from_path(path: Path | str) -> "FSPath":
        return RealFS.from_path(path)

    def __init__(self, fs: FS | Path, path: str | None = None) -> None:
        self.fs: FS
        self.path: str
        if isinstance(fs, Path):
            real_path = RealFS.from_path(fs)
            self.fs = real_path.fs
            self.path = real_path.path
        else:
            assert (
                path is not None
            ), "path must be non None, when not auto converting from Path"
            self.fs = fs
            self.path = path
        self.fs_holder: FS | None = None

    def set_owner(self) -> None:
        self.fs_holder = self.fs

    def is_real_fs(self) -> bool:
        return isinstance(self.fs, RealFS)

    def read_text(self) -> str:
        data = self.read_bytes()
        return data.decode("utf-8")

    def read_bytes(self) -> bytes:
        data: bytes | None = None
        try:
            data = self.fs.read_bytes(self.path)
            return data
        except Exception as e:
            raise FileNotFoundError(f"File not found: {self.path}, because of {e}")

    def exists(self) -> bool:
        return self.fs.exists(self.path)

    def __str__(self) -> str:
        return self.path

    def __repr__(self) -> str:
        return f"FSPath({self.path})"

    def __enter__(self) -> "FSPath":
        if self.fs_holder is not None:
            warnings.warn("This operation is reserved for the cwd returned by FS")
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if self.fs_holder is not None:
            self.fs_holder.dispose()
            self.fs_holder = None

    def mkdir(self, parents=True, exist_ok=True) -> None:
        self.fs.mkdir(self.path, parents=parents, exist_ok=exist_ok)

    def walk(
        self,
    ) -> "Generator[tuple[FSPath, list[str], list[str]], None, None]":
        from rclone_api.fs.walk import fs_walk

        return fs_walk(self)

    def walk_begin(self, max_backlog: int = 8) -> FSWalker:
        """
        Threaded walker to hide latency.

        with FSPath.walk_begin() as walker:
            for root, dirnames, filenames in walker:
                pass
        """
        return FSWalker(self, max_backlog=max_backlog)

    def relative_to(self, other: "FSPath") -> "FSPath":
        p = Path(self.path).relative_to(other.path)
        return FSPath(self.fs, p.as_posix())

    def write_text(self, data: str, encoding: str | None = None) -> None:
        if encoding is None:
            encoding = "utf-8"
        self.write_bytes(data.encode(encoding))

    def moveTo(self, dst: "FSPath") -> None:
        """Move a file or directory."""
        # assert self.exists(), f"Path does not exist: {self.path}"
        # assert not dst.exists(), f"Destination path already exists: {dst.path}"
        self.fs.copy(self.path, dst.path)
        self.fs.remove(self.path)

    def write_bytes(self, data: bytes) -> None:
        self.fs.write_binary(self.path, data)

    def rmtree(self, ignore_errors=False) -> None:
        self_exists = self.exists()
        if not ignore_errors:
            assert self_exists, f"Path does not exist: {self.path}"
        # check fs is RealFS
        # assert isinstance(self.fs, RealFS)
        # shutil.rmtree(self.path, ignore_errors=ignore_errors)
        if isinstance(self.fs, RealFS):
            shutil.rmtree(self.path, ignore_errors=ignore_errors)
            return
        assert isinstance(self.fs, RemoteFS)
        # walk the directory
        for root, _, filenames in self.walk():
            # for dirname in dirnames:
            #     path = root / dirname
            #     path.remove()
            for filename in filenames:
                path = root / filename
                path.remove()

    def lspaths(self) -> "tuple[list[FSPath], list[FSPath]]":
        filenames, dirnames = self.ls()
        fpaths: list[FSPath] = [self / name for name in filenames]
        dpaths: list[FSPath] = [self / name for name in dirnames]
        return fpaths, dpaths

    def ls(self) -> tuple[list[str], list[str]]:
        filenames: list[str]
        dirnames: list[str]
        filenames, dirnames = self.fs.ls(self.path)
        return filenames, dirnames

    def remove(self) -> Exception | None:
        """Remove a file or directory, there are subtle differences between the Real and RemoteFS."""
        return self.fs.remove(self.path)

    def unlink(self) -> Exception | None:
        """Remove a file or symbolic link, there are subtle differences between the Real and RemoteFS."""
        return self.fs.unlink(self.path)

    def with_suffix(self, suffix: str) -> "FSPath":
        return FSPath(self.fs, Path(self.path).with_suffix(suffix).as_posix())

    @property
    def suffix(self) -> str:
        return Path(self.path).suffix

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
        out = hash(f"{repr(self.fs)}:{self.path}")
        return out

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FSPath):
            return False
        if self.fs != other.fs:
            return False
        if self.path != other.path:
            return False
        return True
