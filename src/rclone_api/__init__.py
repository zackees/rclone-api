# Import logging module to activate default configuration

from rclone_api import logging

from .completed_process import CompletedProcess
from .config import Config, Parsed, Section
from .diff import DiffItem, DiffOption, DiffType
from .dir import Dir
from .dir_listing import DirListing
from .file import File
from .filelist import FileList

# Import the configure_logging function to make it available at package level
from .logging import configure_logging
from .process import Process
from .rclone import Rclone, rclone_verbose
from .remote import Remote
from .rpath import RPath
from .s3.types import MultiUploadResult
from .types import ListingOption, Order, SizeResult, SizeSuffix

__all__ = [
    "Rclone",
    "File",
    "Config",
    "Remote",
    "Dir",
    "RPath",
    "DirListing",
    "FileList",
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
    "logging",
]
