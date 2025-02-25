from rclone_api.completed_process import CompletedProcess
from rclone_api.config import Config, Parsed


def _split_remote_path(str_path: str) -> tuple[str, str]:
    """Split the path into the remote and suffix path."""
    if ":" not in str_path:
        raise ValueError(f"Invalid path: {str_path}")
    remote, suffix_path = str_path.split(":", 1)
    return remote, suffix_path


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
    raise NotImplementedError("sftp reget to mount not implemented")
