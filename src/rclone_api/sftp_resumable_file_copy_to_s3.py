import json
import os
import subprocess
import warnings
from dataclasses import dataclass

import boto3
import paramiko

from rclone_api.completed_process import CompletedProcess
from rclone_api.config import Config, Parsed


@dataclass
class _CopyConfig:
    sftp_host: str
    sftp_user: str
    sftp_pass: str
    s3_host: str
    s3_path: str
    s3_access_key: str
    s3_secret_key: str


UPLOAD_METADATA_FILE = "sftp_s3_upload_metadata.json"
PART_SIZE = 5 * 1024 * 1024  # 5MB


def _split_remote_path(str_path: str) -> tuple[str, str]:
    """Split the path into the remote and suffix path."""
    if ":" not in str_path:
        raise ValueError(f"Invalid path: {str_path}")
    remote, suffix_path = str_path.split(":", 1)
    return remote, suffix_path


def _make_copy_config(sftp_path: str, dst_path: str, config: Config) -> _CopyConfig:
    """Create a copy config from the parsed config."""
    src_remote, src_suffix_path = _split_remote_path(sftp_path)
    dst_remote, dst_suffix_path = _split_remote_path(dst_path)
    parsed: Parsed = config.parse()

    try:
        sftp_section = parsed.sections[src_remote]
        s3_section = parsed.sections[dst_remote]

        return _CopyConfig(
            sftp_host=sftp_section["host"],
            sftp_user=sftp_section["user"],
            sftp_pass=sftp_section["pass"],
            s3_host=s3_section["endpoint"],
            s3_path=s3_section["bucket"],
            s3_access_key=s3_section["access_key_id"],
            s3_secret_key=s3_section["secret_access_key"],
        )
    except KeyError as e:
        warnings.warn(f"Expected to find key {e} in config file.")
        raise


def _load_upload_metadata():
    """Load upload metadata from file."""
    try:
        with open(UPLOAD_METADATA_FILE, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def _save_upload_metadata(upload_id, uploaded_parts) -> None:
    """Save upload metadata to file."""
    with open(UPLOAD_METADATA_FILE, "w") as f:
        json.dump({"UploadId": upload_id, "Parts": uploaded_parts}, f)


def _connect_sftp(copy_config: _CopyConfig) -> paramiko.SFTPClient:
    """Establish an SFTP connection."""
    transport = paramiko.Transport((copy_config.sftp_host, 22))
    transport.connect(username=copy_config.sftp_user, password=copy_config.sftp_pass)
    out = paramiko.SFTPClient.from_transport(transport)
    assert out is not None
    return out


def _connect_s3(copy_config: _CopyConfig):
    """Create an S3 client."""
    return boto3.client(
        "s3",
        endpoint_url=copy_config.s3_host,
        aws_access_key_id=copy_config.s3_access_key,
        aws_secret_access_key=copy_config.s3_secret_key,
    )


def _do_resumable_copy(copy_config: _CopyConfig, chunk_size: int) -> None:
    """Perform a resumable file copy from SFTP to S3 using multipart uploads."""

    sftp = _connect_sftp(copy_config)
    s3 = _connect_s3(copy_config)

    filename = os.path.basename(copy_config.s3_path)
    sftp_file = sftp.open(copy_config.s3_path, "rb")
    file_size = sftp.stat(copy_config.s3_path).st_size
    assert file_size is not None
    part_count = (file_size // chunk_size) + (1 if file_size % chunk_size else 0)

    # Check for existing multipart upload
    upload_metadata = _load_upload_metadata()
    upload_id = upload_metadata["UploadId"] if upload_metadata else None
    uploaded_parts = upload_metadata["Parts"] if upload_metadata else {}

    if not upload_id:
        # Start a new multipart upload
        response = s3.create_multipart_upload(Bucket=copy_config.s3_path, Key=filename)
        upload_id = response["UploadId"]
        uploaded_parts = {}

    parts = []

    for part_num in range(1, part_count + 1):
        if str(part_num) in uploaded_parts:
            print(f"Skipping part {part_num}, already uploaded.")
            parts.append(
                {"PartNumber": part_num, "ETag": uploaded_parts[str(part_num)]}
            )
            continue

        print(f"Uploading part {part_num}...")

        sftp_file.seek((part_num - 1) * chunk_size)
        part_data = sftp_file.read(chunk_size)

        response = s3.upload_part(
            Bucket=copy_config.s3_path,
            Key=filename,
            PartNumber=part_num,
            UploadId=upload_id,
            Body=part_data,
        )

        uploaded_parts[str(part_num)] = response["ETag"]
        parts.append({"PartNumber": part_num, "ETag": response["ETag"]})

        _save_upload_metadata(upload_id, uploaded_parts)

    # Complete the multipart upload
    print("Completing multipart upload...")
    s3.complete_multipart_upload(
        Bucket=copy_config.s3_path,
        Key=filename,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )

    # Cleanup
    os.remove(UPLOAD_METADATA_FILE)
    sftp.close()


def sftp_resumable_file_copy_to_s3(
    src_sftp: str, dst_s3: str, config: Config, chunk_size: int
) -> CompletedProcess:
    """Uses a special resumable algorithm to copy files from an SFTP server to an S3 bucket."""

    copy_config = _make_copy_config(src_sftp, dst_s3, config)
    _do_resumable_copy(copy_config, chunk_size)
    ok_completed: subprocess.CompletedProcess = subprocess.CompletedProcess(
        args="",
        returncode=0,
        stdout=b"",
        stderr=b"",
    )
    return CompletedProcess.from_subprocess(ok_completed)
