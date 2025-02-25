import warnings
from dataclasses import dataclass

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


def _split_remote_path(str_path: str) -> tuple[str, str]:
    """Split the path into the remote and suffix path."""
    if ":" not in str_path:
        raise ValueError(f"Invalid path: {str_path}")
    remote, suffix_path = str_path.split(":", 1)
    return remote, suffix_path


def _make_copy_config(sftp_path: str, dst_path: str, config: Config) -> _CopyConfig:
    """Create a copy config from the parsed config."""

    src_remote: str
    src_suffix_path: str
    src_remote, src_suffix_path = _split_remote_path(sftp_path)
    dst_remote: str
    dst_suffix_path: str
    dst_remote, dst_suffix_path = _split_remote_path(dst_path)
    parsed: Parsed = config.parse()
    print(parsed)
    print(src_remote, src_suffix_path)
    print(dst_remote, dst_suffix_path)
    sftp_section = parsed.sections[src_remote]
    s3_section = parsed.sections[dst_remote]

    try:
        sftp_host = sftp_section["host"]
        sftp_user = sftp_section["user"]
        sftp_password = sftp_section["pass"]
        s3_host = s3_section["endpoint"]
        s3_path = s3_section["bucket"]
        s3_access_key = s3_section["access_key_id"]
        s3_secret_key = s3_section["secret_access_key"]

        return _CopyConfig(
            sftp_host=sftp_host,
            sftp_user=sftp_user,
            sftp_pass=sftp_password,
            s3_host=s3_host,
            s3_path=s3_path,
            s3_access_key=s3_access_key,
            s3_secret_key=s3_secret_key,
        )
    except KeyError as e:
        warnings.warn(f"Expected to find key {e} in config file.")
        raise


def sftp_resumable_file_copy_to_s3(
    src_sftp: str, dst_s3: str, config: Config, chunk_size: int
) -> CompletedProcess:
    """Uses a special resumable algorithim to copy files from an sftp server to an s3 bucket."""
    # use the dst path rclone path to construct the mount path.
    # cmd_list: list[str] = ["sftp", "reget", src, str(mount_path)]
    # cp = self._run(cmd_list)
    # return CompletedProcess.from_subprocess(cp)

    copy_config: _CopyConfig = _make_copy_config(src_sftp, dst_s3, config)
    print(copy_config)
    raise NotImplementedError("sftp reget to mount not implemented")
