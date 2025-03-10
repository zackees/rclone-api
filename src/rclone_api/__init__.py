# Import logging module to activate default configuration

from datetime import datetime
from pathlib import Path
from typing import Generator

from rclone_api import log

from .completed_process import CompletedProcess
from .config import Config, Parsed, Section
from .diff import DiffItem, DiffOption, DiffType
from .dir import Dir
from .dir_listing import DirListing
from .file import File, FileItem
from .file_stream import FilesStream
from .filelist import FileList
from .http_server import HttpFetcher, HttpServer, Range

# Import the configure_logging function to make it available at package level
from .log import configure_logging, setup_default_logging
from .mount import Mount
from .process import Process
from .remote import Remote
from .rpath import RPath
from .s3.types import MultiUploadResult
from .types import ListingOption, Order, PartInfo, SizeResult, SizeSuffix

setup_default_logging()


def rclone_verbose(val: bool | None) -> bool:
    from rclone_api.rclone_impl import rclone_verbose as _rclone_verbose

    return _rclone_verbose(val)


class Rclone:
    def __init__(
        self, rclone_conf: Path | Config, rclone_exe: Path | None = None
    ) -> None:
        from rclone_api.rclone_impl import RcloneImpl

        self.impl: RcloneImpl = RcloneImpl(rclone_conf, rclone_exe)

    def webgui(self, other_args: list[str] | None = None) -> Process:
        """Launch the Rclone web GUI."""
        return self.impl.webgui(other_args=other_args)

    def launch_server(
        self,
        addr: str,
        user: str | None = None,
        password: str | None = None,
        other_args: list[str] | None = None,
    ) -> Process:
        """Launch the Rclone server so it can receive commands"""
        return self.impl.launch_server(
            addr=addr, user=user, password=password, other_args=other_args
        )

    def remote_control(
        self,
        addr: str,
        user: str | None = None,
        password: str | None = None,
        capture: bool | None = None,
        other_args: list[str] | None = None,
    ) -> CompletedProcess:
        return self.impl.remote_control(
            addr=addr,
            user=user,
            password=password,
            capture=capture,
            other_args=other_args,
        )

    def obscure(self, password: str) -> str:
        """Obscure a password for use in rclone config files."""
        return self.impl.obscure(password=password)

    def ls_stream(
        self,
        path: str,
        max_depth: int = -1,
        fast_list: bool = False,
    ) -> FilesStream:
        """
        List files in the given path

        Args:
            src: Remote path to list
            max_depth: Maximum recursion depth (-1 for unlimited)
            fast_list: Use fast list (only use when getting THE entire data repository from the root/bucket, or it's small)
        """
        return self.impl.ls_stream(path=path, max_depth=max_depth, fast_list=fast_list)

    def save_to_db(
        self,
        src: str,
        db_url: str,
        max_depth: int = -1,
        fast_list: bool = False,
    ) -> None:
        """
        Save files to a database (sqlite, mysql, postgres)

        Args:
            src: Remote path to list, this will be used to populate an entire table, so always use the root-most path.
            db_url: Database URL, like sqlite:///data.db or mysql://user:pass@localhost/db or postgres://user:pass@localhost/db
            max_depth: Maximum depth to traverse (-1 for unlimited)
            fast_list: Use fast list (only use when getting THE entire data repository from the root/bucket)

        """
        return self.impl.save_to_db(
            src=src, db_url=db_url, max_depth=max_depth, fast_list=fast_list
        )

    def ls(
        self,
        path: Dir | Remote | str | None = None,
        max_depth: int | None = None,
        glob: str | None = None,
        order: Order = Order.NORMAL,
        listing_option: ListingOption = ListingOption.ALL,
    ) -> DirListing:
        return self.impl.ls(
            path=path,
            max_depth=max_depth,
            glob=glob,
            order=order,
            listing_option=listing_option,
        )

    def listremotes(self) -> list[Remote]:
        return self.impl.listremotes()

    def diff(
        self,
        src: str,
        dst: str,
        min_size: (
            str | None
        ) = None,  # e. g. "1MB" - see rclone documentation: https://rclone.org/commands/rclone_check/
        max_size: (
            str | None
        ) = None,  # e. g. "1GB" - see rclone documentation: https://rclone.org/commands/rclone_check/
        diff_option: DiffOption = DiffOption.COMBINED,
        fast_list: bool = True,
        size_only: bool | None = None,
        checkers: int | None = None,
        other_args: list[str] | None = None,
    ) -> Generator[DiffItem, None, None]:
        """Be extra careful with the src and dst values. If you are off by one
        parent directory, you will get a huge amount of false diffs."""
        return self.impl.diff(
            src=src,
            dst=dst,
            min_size=min_size,
            max_size=max_size,
            diff_option=diff_option,
            fast_list=fast_list,
            size_only=size_only,
            checkers=checkers,
            other_args=other_args,
        )

    def walk(
        self,
        path: Dir | Remote | str,
        max_depth: int = -1,
        breadth_first: bool = True,
        order: Order = Order.NORMAL,
    ) -> Generator[DirListing, None, None]:
        """Walk through the given path recursively.

        Args:
            path: Remote path or Remote object to walk through
            max_depth: Maximum depth to traverse (-1 for unlimited)

        Yields:
            DirListing: Directory listing for each directory encountered
        """
        return self.impl.walk(
            path=path, max_depth=max_depth, breadth_first=breadth_first, order=order
        )

    def scan_missing_folders(
        self,
        src: Dir | Remote | str,
        dst: Dir | Remote | str,
        max_depth: int = -1,
        order: Order = Order.NORMAL,
    ) -> Generator[Dir, None, None]:
        """Walk through the given path recursively.

        WORK IN PROGRESS!!

        Args:
            src: Source directory or Remote to walk through
            dst: Destination directory or Remote to walk through
            max_depth: Maximum depth to traverse (-1 for unlimited)

        Yields:
            DirListing: Directory listing for each directory encountered
        """
        return self.impl.scan_missing_folders(
            src=src, dst=dst, max_depth=max_depth, order=order
        )

    def cleanup(
        self, path: str, other_args: list[str] | None = None
    ) -> CompletedProcess:
        """Cleanup any resources used by the Rclone instance."""
        return self.impl.cleanup(path=path, other_args=other_args)

    def copy_to(
        self,
        src: File | str,
        dst: File | str,
        check: bool | None = None,
        verbose: bool | None = None,
        other_args: list[str] | None = None,
    ) -> CompletedProcess:
        """Copy one file from source to destination.

        Warning - slow.

        """
        return self.impl.copy_to(
            src=src, dst=dst, check=check, verbose=verbose, other_args=other_args
        )

    def copy_files(
        self,
        src: str,
        dst: str,
        files: list[str] | Path,
        check: bool | None = None,
        max_backlog: int | None = None,
        verbose: bool | None = None,
        checkers: int | None = None,
        transfers: int | None = None,
        low_level_retries: int | None = None,
        retries: int | None = None,
        retries_sleep: str | None = None,
        metadata: bool | None = None,
        timeout: str | None = None,
        max_partition_workers: int | None = None,
        multi_thread_streams: int | None = None,
        other_args: list[str] | None = None,
    ) -> list[CompletedProcess]:
        """Copy multiple files from source to destination.

        Args:
            payload: Dictionary of source and destination file paths
        """
        return self.impl.copy_files(
            src=src,
            dst=dst,
            files=files,
            check=check,
            max_backlog=max_backlog,
            verbose=verbose,
            checkers=checkers,
            transfers=transfers,
            low_level_retries=low_level_retries,
            retries=retries,
            retries_sleep=retries_sleep,
            metadata=metadata,
            timeout=timeout,
            max_partition_workers=max_partition_workers,
            multi_thread_streams=multi_thread_streams,
            other_args=other_args,
        )

    def copy(
        self,
        src: Dir | str,
        dst: Dir | str,
        check: bool | None = None,
        transfers: int | None = None,
        checkers: int | None = None,
        multi_thread_streams: int | None = None,
        low_level_retries: int | None = None,
        retries: int | None = None,
        other_args: list[str] | None = None,
    ) -> CompletedProcess:
        """Copy files from source to destination.

        Args:
            src: Source directory
            dst: Destination directory
        """
        return self.impl.copy(
            src=src,
            dst=dst,
            check=check,
            transfers=transfers,
            checkers=checkers,
            multi_thread_streams=multi_thread_streams,
            low_level_retries=low_level_retries,
            retries=retries,
            other_args=other_args,
        )

    def purge(self, path: Dir | str) -> CompletedProcess:
        """Purge a directory"""
        return self.impl.purge(path=path)

    def delete_files(
        self,
        files: str | File | list[str] | list[File],
        check: bool | None = None,
        rmdirs=False,
        verbose: bool | None = None,
        max_partition_workers: int | None = None,
        other_args: list[str] | None = None,
    ) -> CompletedProcess:
        """Delete a directory"""
        return self.impl.delete_files(
            files=files,
            check=check,
            rmdirs=rmdirs,
            verbose=verbose,
            max_partition_workers=max_partition_workers,
            other_args=other_args,
        )

    def exists(self, path: Dir | Remote | str | File) -> bool:
        """Check if a file or directory exists."""
        return self.impl.exists(path=path)

    def is_synced(self, src: str | Dir, dst: str | Dir) -> bool:
        """Check if two directories are in sync."""
        return self.impl.is_synced(src=src, dst=dst)

    def modtime(self, src: str) -> str | Exception:
        """Get the modification time of a file or directory."""
        return self.impl.modtime(src=src)

    def modtime_dt(self, src: str) -> datetime | Exception:
        """Get the modification time of a file or directory."""
        return self.impl.modtime_dt(src=src)

    def write_text(
        self,
        text: str,
        dst: str,
    ) -> Exception | None:
        """Write text to a file."""
        return self.impl.write_text(text=text, dst=dst)

    def write_bytes(
        self,
        data: bytes,
        dst: str,
    ) -> Exception | None:
        """Write bytes to a file."""
        return self.impl.write_bytes(data=data, dst=dst)

    def read_bytes(self, src: str) -> bytes | Exception:
        """Read bytes from a file."""
        return self.impl.read_bytes(src=src)

    def read_text(self, src: str) -> str | Exception:
        """Read text from a file."""
        return self.impl.read_text(src=src)

    def copy_file_resumable_s3(
        self,
        src: str,
        dst: str,
        save_state_json: Path,
        chunk_size: SizeSuffix | None = None,
        read_threads: int = 8,
        write_threads: int = 8,
        retries: int = 3,
        verbose: bool | None = None,
        max_chunks_before_suspension: int | None = None,
        backend_log: Path | None = None,
    ) -> MultiUploadResult:
        """For massive files that rclone can't handle in one go, this function will copy the file in chunks to an S3 store"""
        return self.impl.copy_file_resumable_s3(
            src=src,
            dst=dst,
            save_state_json=save_state_json,
            chunk_size=chunk_size,
            read_threads=read_threads,
            write_threads=write_threads,
            retries=retries,
            verbose=verbose,
            max_chunks_before_suspension=max_chunks_before_suspension,
            backend_log=backend_log,
        )

    def copy_bytes(
        self,
        src: str,
        offset: int | SizeSuffix,
        length: int | SizeSuffix,
        outfile: Path,
        other_args: list[str] | None = None,
    ) -> Exception | None:
        """Copy a slice of bytes from the src file to dst."""
        return self.impl.copy_bytes(
            src=src,
            offset=offset,
            length=length,
            outfile=outfile,
            other_args=other_args,
        )

    def copy_dir(
        self, src: str | Dir, dst: str | Dir, args: list[str] | None = None
    ) -> CompletedProcess:
        """Copy a directory from source to destination."""
        # convert src to str, also dst
        return self.impl.copy_dir(src=src, dst=dst, args=args)

    def copy_remote(
        self, src: Remote, dst: Remote, args: list[str] | None = None
    ) -> CompletedProcess:
        """Copy a remote to another remote."""
        return self.impl.copy_remote(src=src, dst=dst, args=args)

    def copy_file_parts(
        self,
        src: str,  # src:/Bucket/path/myfile.large.zst
        dst_dir: str,  # dst:/Bucket/path/myfile.large.zst-parts/part.{part_number:05d}.start-end
        part_infos: list[PartInfo] | None = None,
        threads: int = 1,  # Number of reader and writer threads to use
    ) -> Exception | None:
        """Copy a file in parts."""
        return self.impl.copy_file_parts(
            src=src, dst_dir=dst_dir, part_infos=part_infos, threads=threads
        )

    def mount(
        self,
        src: Remote | Dir | str,
        outdir: Path,
        allow_writes: bool | None = False,
        use_links: bool | None = None,
        vfs_cache_mode: str | None = None,
        verbose: bool | None = None,
        cache_dir: Path | None = None,
        cache_dir_delete_on_exit: bool | None = None,
        log: Path | None = None,
        other_args: list[str] | None = None,
    ) -> Mount:
        """Mount a remote or directory to a local path.

        Args:
            src: Remote or directory to mount
            outdir: Local path to mount to

        Returns:
            CompletedProcess from the mount command execution

        Raises:
            subprocess.CalledProcessError: If the mount operation fails
        """
        return self.impl.mount(
            src=src,
            outdir=outdir,
            allow_writes=allow_writes,
            use_links=use_links,
            vfs_cache_mode=vfs_cache_mode,
            verbose=verbose,
            cache_dir=cache_dir,
            cache_dir_delete_on_exit=cache_dir_delete_on_exit,
            log=log,
            other_args=other_args,
        )

    def serve_http(
        self,
        src: str,
        addr: str = "localhost:8080",
        other_args: list[str] | None = None,
    ) -> HttpServer:
        """Serve a remote or directory via HTTP. The returned HttpServer has a client which can be used to
        fetch files or parts.

        Args:
            src: Remote or directory to serve
            addr: Network address and port to serve on (default: localhost:8080)
        """
        return self.impl.serve_http(src=src, addr=addr, other_args=other_args)

    def size_files(
        self,
        src: str,
        files: list[str],
        fast_list: bool = False,  # Recommend that this is False
        other_args: list[str] | None = None,
        check: bool | None = False,
        verbose: bool | None = None,
    ) -> SizeResult:
        """Get the size of a list of files. Example of files items: "remote:bucket/to/file"."""
        return self.impl.size_files(
            src=src,
            files=files,
            fast_list=fast_list,
            other_args=other_args,
            check=check,
            verbose=verbose,
        )


__all__ = [
    "Rclone",
    "File",
    "Config",
    "Remote",
    "Dir",
    "RPath",
    "DirListing",
    "FileList",
    "FileItem",
    "Process",
    "DiffItem",
    "DiffType",
    "rclone_verbose",
    "CompletedProcess",
    "DiffOption",
    "ListingOption",
    "Order",
    "ListingOption",
    "SizeResult",
    "Parsed",
    "Section",
    "MultiUploadResult",
    "SizeSuffix",
    "configure_logging",
    "log",
    "HttpServer",
    "Range",
    "HttpFetcher",
    "PartInfo",
]
