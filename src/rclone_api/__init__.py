"""
Rclone API - Python interface for the Rclone command-line tool.

This package provides a high-level API for interacting with Rclone,
allowing file operations across various cloud storage providers.
The API wraps the rclone command-line tool, providing a Pythonic interface
for common operations like copying, listing, and managing remote storage.
"""

# Import core components and utilities
from datetime import datetime
from pathlib import Path
from typing import Generator

# Import logging utilities
from rclone_api import log

# Import data structures and models
from .completed_process import CompletedProcess
from .config import Config, Parsed, Section  # Configuration handling
from .diff import DiffItem, DiffOption, DiffType  # File comparison utilities
from .dir import Dir  # Directory representation
from .dir_listing import DirListing  # Directory contents representation
from .file import File, FileItem  # File representation
from .file_stream import FilesStream  # Streaming file listings
from .filelist import FileList  # File list utilities
from .fs import FSPath, RealFS, RemoteFS  # Filesystem utilities
from .http_server import HttpFetcher, HttpServer, Range  # HTTP serving capabilities

# Import logging configuration utilities
from .log import configure_logging, setup_default_logging
from .mount import Mount  # Mount remote filesystems
from .process import Process  # Process management
from .remote import Remote  # Remote storage representation
from .rpath import RPath  # Remote path utilities
from .s3.types import MultiUploadResult  # S3-specific types
from .types import (  # Common types
    ListingOption,
    Order,
    PartInfo,
    SizeResult,
    SizeSuffix,
)

# Set up default logging configuration when the package is imported
setup_default_logging()


def rclone_verbose(val: bool | None) -> bool:
    """
    Get or set the global verbosity setting for rclone operations.

    Controls whether rclone commands will produce detailed output.
    When enabled, commands will show more information about their operation.

    Args:
        val: If provided, sets the verbosity level. If None, returns the current setting.

    Returns:
        The current verbosity setting after any change.
    """
    from rclone_api.rclone_impl import rclone_verbose as _rclone_verbose

    return _rclone_verbose(val)


class Rclone:
    """
    Main interface for interacting with Rclone.

    This class provides methods for all major Rclone operations including
    file transfers, listing, mounting, and remote management.

    It serves as the primary entry point for the API, wrapping the underlying
    implementation details and providing a clean, consistent interface.
    """

    @staticmethod
    def upgrade_rclone() -> Path:
        """
        Upgrade the rclone executable to the latest version.

        Downloads the latest rclone binary and replaces the current one.

        If an external rclone is already in your path then although upgrade_rclone
        will download the latest version, it will not affect the rclone selected.

        Returns:
            Path to the upgraded rclone executable
        """
        from rclone_api.util import upgrade_rclone

        return upgrade_rclone()

    @staticmethod
    def find_rclone_conf() -> Path | None:
        """Find the rclone configuration file from the environment or default locations."""
        from rclone_api.config import find_conf_file

        return find_conf_file()

    def __init__(
        self, rclone_conf: Path | Config | None, rclone_exe: Path | None = None
    ) -> None:
        """
        Initialize the Rclone interface.

        Args:
            rclone_conf: Path to rclone config file or Config object
            rclone_exe: Optional path to rclone executable. If None, will search in PATH.
        """
        from rclone_api.rclone_impl import RcloneImpl

        self.impl: RcloneImpl = RcloneImpl(rclone_conf, rclone_exe)

    def webgui(self, other_args: list[str] | None = None) -> Process:
        """
        Launch the Rclone web GUI.

        Starts the built-in web interface for interacting with rclone.

        Args:
            other_args: Additional command-line arguments to pass to rclone

        Returns:
            Process object representing the running web GUI
        """
        return self.impl.webgui(other_args=other_args)

    def filesystem(self, src: str) -> RemoteFS:
        """
        Get a RealFS object for interacting with the local filesystem.

        Returns:
            RealFS object for local filesystem operations
        """
        return self.impl.filesystem(src=src)

    def cwd(self, src: str) -> FSPath:
        """
        Get the local root path for a filesystem.

        Args:
            src: Source path for the filesystem

        Returns:
            FSPath object representing the root of the filesystem
        """
        return self.impl.cwd(src=src)

    def launch_server(
        self,
        addr: str,
        user: str | None = None,
        password: str | None = None,
        other_args: list[str] | None = None,
    ) -> Process:
        """
        Launch the Rclone server so it can receive commands.

        Starts an rclone server that can be controlled remotely.

        Args:
            addr: Address and port to listen on (e.g., "localhost:5572")
            user: Optional username for authentication
            password: Optional password for authentication
            other_args: Additional command-line arguments

        Returns:
            Process object representing the running server
        """
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
        """
        Send commands to a running rclone server.

        Args:
            addr: Address of the rclone server (e.g., "localhost:5572")
            user: Optional username for authentication
            password: Optional password for authentication
            capture: Whether to capture and return command output
            other_args: Additional command-line arguments

        Returns:
            CompletedProcess containing the command result
        """
        return self.impl.remote_control(
            addr=addr,
            user=user,
            password=password,
            capture=capture,
            other_args=other_args,
        )

    def obscure(self, password: str) -> str:
        """
        Obscure a password for use in rclone config files.

        Converts a plaintext password to rclone's obscured format.
        Note that this is not secure encryption, just light obfuscation.

        Args:
            password: The plaintext password to obscure

        Returns:
            The obscured password string
        """
        return self.impl.obscure(password=password)

    def config_show(
        self, remote: str | None = None, obscure: bool = False, no_obscure: bool = False
    ) -> list[Path] | Exception:
        """Show the current configuration.

        Args:
            remote: Optional remote name to show configuration for
            obscure: Show obscured passwords
            no_obscure: Show passwords in plain text

        Returns:
            Configuration as text or an Exception if an error occurred
        """
        return self.impl.config_paths(
            remote=remote, obscure=obscure, no_obscure=no_obscure
        )

    def ls_stream(
        self,
        src: str,
        max_depth: int = -1,
        fast_list: bool = False,
    ) -> FilesStream:
        """
        List files in the given path as a stream of results.

        This method is memory-efficient for large directories as it yields
        results incrementally rather than collecting them all at once.

        Args:
            src: Remote path to list
            max_depth: Maximum recursion depth (-1 for unlimited)
            fast_list: Use fast list (only recommended for listing entire repositories or small datasets)

        Returns:
            A stream of file entries that can be iterated over
        """
        return self.impl.ls_stream(src=src, max_depth=max_depth, fast_list=fast_list)

    def save_to_db(
        self,
        src: str,
        db_url: str,  # sqalchemy style url, use sqlite:///data.db or mysql://user:pass@localhost/db or postgres://user:pass@localhost/db
        max_depth: int = -1,
        fast_list: bool = False,
    ) -> None:
        """
        Save files to a database (sqlite, mysql, postgres).

        Lists all files in the source path and stores their metadata in a database.
        Useful for creating searchable indexes of remote storage.

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
        src: Dir | Remote | str | None = None,
        max_depth: int | None = None,
        glob: str | None = None,
        order: Order = Order.NORMAL,
        listing_option: ListingOption = ListingOption.ALL,
    ) -> DirListing:
        """
        List files and directories at the specified path.

        Provides a detailed listing with file metadata.

        Args:
            src: Path to list (Dir, Remote, or string path)
            max_depth: Maximum recursion depth (None for default)
            glob: Optional glob pattern to filter results
            order: Sorting order for the results
            listing_option: What types of entries to include

        Returns:
            DirListing object containing the results
        """
        return self.impl.ls(
            src=src,
            max_depth=max_depth,
            glob=glob,
            order=order,
            listing_option=listing_option,
        )

    def listremotes(self) -> list[Remote]:
        """
        List all configured remotes.

        Returns a list of all remotes defined in the rclone configuration.

        Returns:
            List of Remote objects
        """
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
        """
        Compare two directories and yield differences.

        Be extra careful with the src and dst values. If you are off by one
        parent directory, you will get a huge amount of false diffs.

        Args:
            src: Rclone style src path
            dst: Rclone style dst path
            min_size: Minimum file size to check (e.g., "1MB")
            max_size: Maximum file size to check (e.g., "1GB")
            diff_option: How to report differences
            fast_list: Whether to use fast listing
            size_only: Compare only file sizes, not content
            checkers: Number of checker threads
            other_args: Additional command-line arguments

        Yields:
            DiffItem objects representing each difference found
        """
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
        src: Dir | Remote | str,
        max_depth: int = -1,
        breadth_first: bool = True,
        order: Order = Order.NORMAL,
    ) -> Generator[DirListing, None, None]:
        """
        Walk through the given path recursively, yielding directory listings.

        Similar to os.walk(), but for remote storage. Traverses directories
        and yields their contents.

        Args:
            src: Remote path, Dir, or Remote object to walk through
            max_depth: Maximum depth to traverse (-1 for unlimited)
            breadth_first: If True, use breadth-first traversal, otherwise depth-first
            order: Sorting order for directory entries

        Yields:
            DirListing: Directory listing for each directory encountered
        """
        return self.impl.walk(
            src=src, max_depth=max_depth, breadth_first=breadth_first, order=order
        )

    def scan_missing_folders(
        self,
        src: Dir | Remote | str,
        dst: Dir | Remote | str,
        max_depth: int = -1,
        order: Order = Order.NORMAL,
    ) -> Generator[Dir, None, None]:
        """
        Find folders that exist in source but are missing in destination.

        Useful for identifying directories that need to be created before
        copying files.

        Args:
            src: Source directory or Remote to scan
            dst: Destination directory or Remote to compare against
            max_depth: Maximum depth to traverse (-1 for unlimited)
            order: Sorting order for directory entries

        Yields:
            Dir: Each directory that exists in source but not in destination
        """
        return self.impl.scan_missing_folders(
            src=src, dst=dst, max_depth=max_depth, order=order
        )

    def cleanup(
        self, src: str, other_args: list[str] | None = None
    ) -> CompletedProcess:
        """
        Cleanup any resources used by the Rclone instance.

        Removes temporary files and directories created by rclone.

        Args:
            src: Path to clean up
            other_args: Additional command-line arguments

        Returns:
            CompletedProcess with the result of the cleanup operation
        """
        return self.impl.cleanup(src=src, other_args=other_args)

    def copy_to(
        self,
        src: File | str,
        dst: File | str,
        check: bool | None = None,
        verbose: bool | None = None,
        other_args: list[str] | None = None,
    ) -> CompletedProcess:
        """
        Copy one file from source to destination.

        Warning - this can be slow for large files or when copying between
        different storage providers.

        Args:
            src: Rclone style src path
            dst: Rclone style dst path
            check: Whether to verify the copy with checksums
            verbose: Whether to show detailed progress
            other_args: Additional command-line arguments

        Returns:
            CompletedProcess with the result of the copy operation
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
        """
        Copy multiple files from source to destination.

        Efficiently copies a list of files, potentially in parallel.

        Args:
            src: Rclone style src path
            dst: Rclone style dst path
            files: List of file paths relative to src, or Path to a file containing the list
            check: Whether to verify copies with checksums
            max_backlog: Maximum number of queued transfers
            verbose: Whether to show detailed progress
            checkers: Number of checker threads
            transfers: Number of file transfers to run in parallel
            low_level_retries: Number of low-level retries
            retries: Number of high-level retries
            retries_sleep: Sleep interval between retries (e.g., "10s")
            metadata: Whether to preserve metadata
            timeout: IO idle timeout (e.g., "5m")
            max_partition_workers: Maximum number of partition workers
            multi_thread_streams: Number of streams for multi-thread copy
            other_args: Additional command-line arguments

        Returns:
            List of CompletedProcess objects for each copy operation
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
        """
        Copy files from source to destination.

        Recursively copies all files from src to dst.

        Args:
            src: Rclone style src path
            dst: Rclone style dst path
            check: Whether to verify copies with checksums
            transfers: Number of file transfers to run in parallel
            checkers: Number of checker threads
            multi_thread_streams: Number of streams for multi-thread copy
            low_level_retries: Number of low-level retries
            retries: Number of high-level retries
            other_args: Additional command-line arguments

        Returns:
            CompletedProcess with the result of the copy operation
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

    def purge(self, src: Dir | str) -> CompletedProcess:
        """
        Purge a directory.

        Removes a directory and all its contents.

        Args:
            src: Rclone style path

        Returns:
            CompletedProcess with the result of the purge operation
        """
        return self.impl.purge(src=src)

    def delete_files(
        self,
        files: str | File | list[str] | list[File],
        check: bool | None = None,
        rmdirs=False,
        verbose: bool | None = None,
        max_partition_workers: int | None = None,
        other_args: list[str] | None = None,
    ) -> CompletedProcess:
        """
        Delete files or directories.

        Args:
            files: Files to delete (single file/path or list)
            check: Whether to verify deletions
            rmdirs: Whether to remove empty directories
            verbose: Whether to show detailed progress
            max_partition_workers: Maximum number of partition workers
            other_args: Additional command-line arguments

        Returns:
            CompletedProcess with the result of the delete operation
        """
        return self.impl.delete_files(
            files=files,
            check=check,
            rmdirs=rmdirs,
            verbose=verbose,
            max_partition_workers=max_partition_workers,
            other_args=other_args,
        )

    def exists(self, src: Dir | Remote | str | File) -> bool:
        """
        Check if a file or directory exists.

        Args:
            src: Path to check (Dir, Remote, File, or path string)

        Returns:
            True if the path exists, False otherwise
        """
        return self.impl.exists(src=src)

    def is_synced(self, src: str | Dir, dst: str | Dir) -> bool:
        """
        Check if two directories are in sync.

        Compares the contents of src and dst to determine if they match.

        Args:
            src: Source directory (Dir object or path string)
            dst: Destination directory (Dir object or path string)

        Returns:
            True if the directories are in sync, False otherwise
        """
        return self.impl.is_synced(src=src, dst=dst)

    def modtime(self, src: str) -> str | Exception:
        """
        Get the modification time of a file or directory.

        Args:
            src: Path to the file or directory

        Returns:
            Modification time as a string, or Exception if an error occurred
        """
        return self.impl.modtime(src=src)

    def modtime_dt(self, src: str) -> datetime | Exception:
        """
        Get the modification time of a file or directory as a datetime object.

        Args:
            src: Path to the file or directory

        Returns:
            Modification time as a datetime object, or Exception if an error occurred
        """
        return self.impl.modtime_dt(src=src)

    def write_text(
        self,
        text: str,
        dst: str,
    ) -> Exception | None:
        """
        Write text to a file.

        Creates or overwrites the file at dst with the given text.

        Args:
            text: Text content to write
            dst: Destination file path

        Returns:
            None if successful, Exception if an error occurred
        """
        return self.impl.write_text(text=text, dst=dst)

    def write_bytes(
        self,
        data: bytes,
        dst: str,
    ) -> Exception | None:
        """
        Write bytes to a file.

        Creates or overwrites the file at dst with the given binary data.

        Args:
            data: Binary content to write
            dst: Destination file path

        Returns:
            None if successful, Exception if an error occurred
        """
        return self.impl.write_bytes(data=data, dst=dst)

    def read_bytes(self, src: str) -> bytes | Exception:
        """
        Read bytes from a file.

        Args:
            src: Source file path

        Returns:
            File contents as bytes, or Exception if an error occurred
        """
        return self.impl.read_bytes(src=src)

    def read_text(self, src: str) -> str | Exception:
        """
        Read text from a file.

        Args:
            src: Source file path

        Returns:
            File contents as a string, or Exception if an error occurred
        """
        return self.impl.read_text(src=src)

    def copy_bytes(
        self,
        src: str,
        offset: int | SizeSuffix,
        length: int | SizeSuffix,
        outfile: Path,
        other_args: list[str] | None = None,
    ) -> Exception | None:
        """
        Copy a slice of bytes from the src file to dst.

        Extracts a portion of a file based on offset and length.

        Args:
            src: Source file path
            offset: Starting position in the source file
            length: Number of bytes to copy
            outfile: Local file path to write the bytes to
            other_args: Additional command-line arguments

        Returns:
            None if successful, Exception if an error occurred
        """
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
        """
        Copy a directory from source to destination.

        Recursively copies all files and subdirectories.

        Args:
            src: Source directory (Dir object or path string)
            dst: Destination directory (Dir object or path string)
            args: Additional command-line arguments

        Returns:
            CompletedProcess with the result of the copy operation
        """
        # convert src to str, also dst
        return self.impl.copy_dir(src=src, dst=dst, args=args)

    def copy_remote(
        self, src: Remote, dst: Remote, args: list[str] | None = None
    ) -> CompletedProcess:
        """
        Copy a remote to another remote.

        Copies all contents from one remote storage to another.

        Args:
            src: Source remote
            dst: Destination remote
            args: Additional command-line arguments

        Returns:
            CompletedProcess with the result of the copy operation
        """
        return self.impl.copy_remote(src=src, dst=dst, args=args)

    def copy_file_s3_resumable(
        self,
        src: str,  # src:/Bucket/path/myfile.large.zst
        dst: str,  # dst:/Bucket/path/myfile.large.zst
        part_infos: list[PartInfo] | None = None,
        upload_threads: int = 8,  # Number of reader and writer threads to use
        merge_threads: int = 4,  # Number of threads to use for merging the parts
    ) -> Exception | None:
        """
        Copy a large file to S3 with resumable upload capability.

        This method splits the file into parts for parallel upload and can
        resume interrupted transfers using a custom algorithm in python.

        Particularly useful for very large files where network interruptions
        are likely.

        Args:
            src: Source file path (format: remote:bucket/path/file)
            dst: Destination file path (format: remote:bucket/path/file)
            part_infos: Optional list of part information for resuming uploads
            upload_threads: Number of parallel upload threads
            merge_threads: Number of threads for merging uploaded parts

        Returns:
            None if successful, Exception if an error occurred
        """
        return self.impl.copy_file_s3_resumable(
            src=src,
            dst=dst,
            part_infos=part_infos,
            upload_threads=upload_threads,
            merge_threads=merge_threads,
        )

    def mount(
        self,
        src: Remote | Dir | str,
        outdir: Path,
        allow_writes: bool | None = False,
        transfers: int | None = None,  # number of writes to perform in parallel
        use_links: bool | None = None,
        vfs_cache_mode: str | None = None,
        verbose: bool | None = None,
        cache_dir: Path | None = None,
        cache_dir_delete_on_exit: bool | None = None,
        log: Path | None = None,
        other_args: list[str] | None = None,
    ) -> Mount:
        """
        Mount a remote or directory to a local path.

        Makes remote storage accessible as a local filesystem.

        Args:
            src: Remote or directory to mount
            outdir: Local path to mount to
            allow_writes: Whether to allow write operations
            transfers: Number of parallel write operations
            use_links: Whether to use symbolic links
            vfs_cache_mode: VFS cache mode (e.g., "full", "minimal")
            verbose: Whether to show detailed output
            cache_dir: Directory to use for caching
            cache_dir_delete_on_exit: Whether to delete cache on exit
            log: Path to write logs to
            other_args: Additional command-line arguments

        Returns:
            Mount object representing the mounted filesystem
        """
        return self.impl.mount(
            src=src,
            outdir=outdir,
            allow_writes=allow_writes,
            transfers=transfers,
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
        addr: str | None = None,
        other_args: list[str] | None = None,
    ) -> HttpServer:
        """
        Serve a remote or directory via HTTP.

        Creates an HTTP server that provides access to the specified remote.
        The returned HttpServer object includes a client for fetching files.

        This is useful for providing web access to remote storage or for
        accessing remote files from applications that support HTTP but not
        the remote's native protocol.

        Args:
            src: Remote or directory to serve
            addr: Network address and port to serve on (default: localhost:8080)
            other_args: Additional arguments to pass to rclone

        Returns:
            HttpServer object with methods for accessing the served content
        """
        return self.impl.serve_http(
            src=src, cache_mode="minimal", addr=addr, other_args=other_args
        )

    def size_files(
        self,
        src: str,
        files: list[str],
        fast_list: bool = False,  # Recommend that this is False
        other_args: list[str] | None = None,
        check: bool | None = False,
        verbose: bool | None = None,
    ) -> SizeResult | Exception:
        """
        Get the size of a list of files.

        Calculates the total size of the specified files.

        Args:
            src: Base path for the files
            files: List of file paths relative to src
            fast_list: Whether to use fast listing (not recommended for accuracy)
            other_args: Additional command-line arguments
            check: Whether to verify file integrity
            verbose: Whether to show detailed output

        Returns:
            SizeResult with size information, or Exception if an error occurred

        Example:
            size_files("remote:bucket", ["path/to/file1", "path/to/file2"])
        """
        return self.impl.size_files(
            src=src,
            files=files,
            fast_list=fast_list,
            other_args=other_args,
            check=check,
            verbose=verbose,
        )

    def size_file(self, src: str) -> SizeSuffix | Exception:
        """
        Get the size of a file.

        Args:
            src: Path to the file

        Returns:
            SizeSuffix object representing the file size, or Exception if an error occurred
        """
        return self.impl.size_file(src=src)


# Export public API components
__all__ = [
    # Main classes
    "Rclone",  # Primary interface
    "File",  # File representation
    "Config",  # Configuration handling
    "Remote",  # Remote storage
    "Dir",  # Directory representation
    "RPath",  # Remote path utilities
    "DirListing",  # Directory listing
    "FileList",  # File list
    "FileItem",  # File item
    "Process",  # Process management
    "DiffItem",  # Difference item
    "DiffType",  # Difference type
    # Functions
    "rclone_verbose",  # Verbosity control
    # Data classes and enums
    "CompletedProcess",  # Process result
    "DiffOption",  # Difference options
    "ListingOption",  # Listing options
    "Order",  # Sorting order
    "SizeResult",  # Size result
    "Parsed",  # Parsed configuration
    "Section",  # Configuration section
    "MultiUploadResult",  # S3 upload result
    "SizeSuffix",  # Size with suffix
    # Utilities
    "configure_logging",  # Logging configuration
    "log",  # Logging utilities
    "HttpServer",  # HTTP server
    "Range",  # HTTP range
    "HttpFetcher",  # HTTP fetcher
    "PartInfo",  # Part information for uploads
    "RealFS",  # Real filesystem
    "RemoteFS",  # Remote filesystem
    "FSPath",  # File object backed by a real or remote filesystem
    "FilesStream",
]
