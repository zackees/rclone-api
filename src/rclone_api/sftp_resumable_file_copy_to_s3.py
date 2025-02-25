from dataclasses import dataclass

from rclone_api.completed_process import CompletedProcess
from rclone_api.config import Config, Parsed


@dataclass
class _CopyConfig:
    sftp_host: str
    sftp_user: str
    sftp_password: str

    s3_host: str
    s3_path: str
    s3_access_key: str
    s3_secret_key: str


def _split_remote_path(str_path: str) -> tuple[str, str]:
    """Split the path into the remote and suffix path."""
    if ":" not in str_path:
        raise ValueError(f"Invalid path: {str_path}")
    remote, suffix_path = str_path.split(":", 1)
    return remote, suffix_path


def _make_copy_config(config: Config) -> _CopyConfig:
    """Create a copy config from the parsed config."""
    parsed: Parsed = config.parse()
    sftp_host = parsed.sections["src"]["host"]
    sftp_user = parsed.sections["src"]["user"]
    sftp_password = parsed.sections["src"]["password"]
    s3_host = parsed.sections["dst"]["endpoint"]
    s3_path = parsed.sections["dst"]["bucket"]
    s3_access_key = parsed.sections["dst"]["access_key_id"]
    s3_secret_key = parsed.sections["dst"]["secret_access_key"]
    return _CopyConfig(
        sftp_host=sftp_host,
        sftp_user=sftp_user,
        sftp_password=sftp_password,
        s3_host=s3_host,
        s3_path=s3_path,
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key,
    )


def sftp_resumable_file_copy_to_s3(
    src_sftp: str, dst_s3: str, config: Config, chunk_size: int
) -> CompletedProcess:
    """Uses a special resumable algorithim to copy files from an sftp server to an s3 bucket."""
    # use the dst path rclone path to construct the mount path.
    # cmd_list: list[str] = ["sftp", "reget", src, str(mount_path)]
    # cp = self._run(cmd_list)
    # return CompletedProcess.from_subprocess(cp)
    src_remote: str
    src_path: str
    src_remote, src_path = _split_remote_path(src_sftp)
    dst_remote: str
    dst_path: str
    dst_remote, dst_path = _split_remote_path(dst_s3)
    parsed: Parsed = config.parse()
    print(parsed)
    print(src_remote, src_path)
    print(dst_remote, dst_path)
    copy_config: _CopyConfig = _make_copy_config(config)
    print(copy_config)
    raise NotImplementedError("sftp reget to mount not implemented")
