from rclone_api.completed_process import CompletedProcess
from rclone_api.config import Config, Parsed


def sftp_resumable_file_copy_to_s3(
    src: str, dst: str, config: Config, chunk_size: int
) -> CompletedProcess:
    """Uses a special resumable algorithim to copy files from an sftp server to an s3 bucket."""
    # use the dst path rclone path to construct the mount path.
    # cmd_list: list[str] = ["sftp", "reget", src, str(mount_path)]
    # cp = self._run(cmd_list)
    # return CompletedProcess.from_subprocess(cp)
    parsed: Parsed = config.parse()
    print(parsed)
    raise NotImplementedError("sftp reget to mount not implemented")
