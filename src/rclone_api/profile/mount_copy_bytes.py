"""
Unit test file.
"""

import argparse
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

import psutil
from dotenv import load_dotenv

from rclone_api import Config, Rclone, SizeSuffix

os.environ["RCLONE_API_VERBOSE"] = "1"


@dataclass
class Args:
    direct_io: bool
    num: int
    size: SizeSuffix | None


@dataclass
class Credentials:
    BUCKET_KEY_SECRET: str
    BUCKET_KEY_PUBLIC: str
    BUCKET_NAME: str
    SRC_SFTP_HOST: str
    SRC_SFTP_USER: str
    SRC_SFTP_PORT: str
    SRC_SFTP_PASS: str
    BUCKET_URL: str


def _generate_rclone_config() -> tuple[Config, Credentials]:

    cwd = Path.cwd()
    env_path = cwd / ".env"
    assert (
        env_path.exists()
    ), "this test requires that the secret .env file exists with the credentials"
    load_dotenv(env_path, verbose=True)
    print(f"Current working directory: {cwd}")

    # assert that .env exists for this test
    assert os.path.exists(
        ".env"
    ), "this test requires that the secret .env file exists with the credentials"

    # BUCKET_NAME = os.getenv("BUCKET_NAME", "TorrentBooks")  # Default if not in .env

    # Load additional environment variables
    BUCKET_KEY_SECRET = os.getenv("BUCKET_KEY_SECRET")
    BUCKET_KEY_PUBLIC = os.getenv("BUCKET_KEY_PUBLIC")
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    SRC_SFTP_HOST = os.getenv("SRC_SFTP_HOST")
    SRC_SFTP_USER = os.getenv("SRC_SFTP_USER")
    SRC_SFTP_PORT = os.getenv("SRC_SFTP_PORT")
    SRC_SFTP_PASS = os.getenv("SRC_SFTP_PASS")
    # BUCKET_URL = os.getenv("BUCKET_URL")
    BUCKET_URL = "sfo3.digitaloceanspaces.com"

    config_text = f"""
[dst]
type = s3
provider = DigitalOcean
access_key_id = {BUCKET_KEY_PUBLIC}
secret_access_key = {BUCKET_KEY_SECRET}
endpoint = {BUCKET_URL}
bucket = {BUCKET_NAME}

[src]
type = sftp
host = {SRC_SFTP_HOST}
user = {SRC_SFTP_USER}
port = {SRC_SFTP_PORT}
pass = {SRC_SFTP_PASS}

"""
    # print("Config text:")
    # print(config_text)
    # _CONFIG_PATH.write_text(config_text, encoding="utf-8")
    # print(f"Config file written to: {_CONFIG_PATH}")

    creds = Credentials(
        BUCKET_KEY_SECRET=str(BUCKET_KEY_SECRET),
        BUCKET_KEY_PUBLIC=str(BUCKET_KEY_PUBLIC),
        BUCKET_NAME=str(BUCKET_NAME),
        SRC_SFTP_HOST=str(SRC_SFTP_HOST),
        SRC_SFTP_USER=str(SRC_SFTP_USER),
        SRC_SFTP_PORT=str(SRC_SFTP_PORT),
        SRC_SFTP_PASS=str(SRC_SFTP_PASS),
        BUCKET_URL=str(BUCKET_URL),
    )

    return Config(config_text), creds


def _run_profile(
    rclone: Rclone,
    src_file: str,
    transfers: int,
    offset: SizeSuffix,
    size: SizeSuffix,
    log_dir: Path,
    num: int,
    direct_io: bool,
) -> None:

    mount_log = log_dir / f"mount_{SizeSuffix(size)}_threads_{transfers}.log"
    print("\n\n")
    print("#" * 80)
    print(f"# Started test download of {SizeSuffix(size)} with {transfers} transfers")
    print("#" * 80)
    net_io_start = psutil.net_io_counters()
    start = time.time()
    chunk_size = size // transfers
    bytes_or_err: bytes | Exception = rclone.copy_bytes(
        src=src_file,
        offset=offset.as_int(),
        length=size.as_int(),
        chunk_size=chunk_size,
        direct_io=direct_io,
        max_threads=transfers,
        mount_log=mount_log,
    )
    diff = time.time() - start
    net_io_end = psutil.net_io_counters()
    if isinstance(bytes_or_err, Exception):
        print(bytes_or_err)
        stack_trace = bytes_or_err.__traceback__
        print(f"Error: {bytes_or_err}\nStack trace:\n{stack_trace}")
        assert False, f"Error: {bytes_or_err}\nStack trace:\n{stack_trace}"
    assert isinstance(bytes_or_err, bytes)
    # self.assertEqual(len(bytes_or_err), size)
    assert len(bytes_or_err) == size.as_int(), f"Length: {len(bytes_or_err)} != {size}"

    # print io stats
    # disabling num for now
    num = 1
    bytes_sent = (net_io_end.bytes_sent - net_io_start.bytes_sent) // num
    bytes_recv = (net_io_end.bytes_recv - net_io_start.bytes_recv) // num
    packets_sent = (net_io_end.packets_sent - net_io_start.packets_sent) // num
    efficiency = size.as_int() / (bytes_recv)
    efficiency_100 = efficiency * 100
    efficiency_str = f"{efficiency_100:.2f}"

    bytes_send_suffix = SizeSuffix(bytes_sent)
    bytes_recv_suffix = SizeSuffix(bytes_recv)
    range_size = SizeSuffix(size)

    print(f"\nFinished downloading {range_size} with {transfers} transfers")
    print("Net IO stats:")
    print(f"Bytes sent: {bytes_send_suffix}")
    print(f"Bytes received: {bytes_recv_suffix}")
    print(f"Packets sent: {packets_sent}")
    print(f"Efficiency: {efficiency_str}%")
    print(f"Time: {diff:.1f} seconds")


def test_profile_copy_bytes(
    args: Args,
    rclone: Rclone,
    offset: SizeSuffix,
    transfer_list: list[int] | None,
    mount_root_path: Path,
    size: SizeSuffix | None,
    num: int,
) -> None:

    if size:
        sizes = [size.as_int()]
    else:
        sizes = [
            # 1024 * 1024 * 1,
            # 1024 * 1024 * 2,
            # 1024 * 1024 * 4,
            # 1024 * 1024 * 8,
            1024 * 1024 * 16,
            # 1024 * 1024 * 32,
            1024 * 1024 * 64,
            1024 * 1024 * 128,
            # 1024 * 1024 * 256,
        ]
    # transfer_list = [1, 2, 4, 8, 16]
    transfer_list = transfer_list or [1, 2, 4]

    # src_file = "dst:rclone-api-unit-test/zachs_video/internaly_ai_alignment.mp4"
    # sftp mount
    src_file = "src:aa_misc_data/aa_misc_data/world_lending_library_2024_11.tar.zst"

    for sz in sizes:
        for transfers in transfer_list:
            _run_profile(
                rclone=rclone,
                src_file=src_file,
                transfers=transfers,
                offset=offset,
                size=SizeSuffix(sz),
                direct_io=args.direct_io,
                log_dir=mount_root_path,
                num=num,
            )
    print("done")


def _parse_args() -> Args:
    parser = argparse.ArgumentParser(description="Profile copy_bytes")
    parser.add_argument("--direct-io", help="Use direct IO", action="store_true")
    parser.add_argument("-n", "--num", help="Number of workers", type=int, default=1)
    parser.add_argument(
        "--size", help="Size of the file to download", type=SizeSuffix, default=None
    )
    args = parser.parse_args()
    return Args(direct_io=args.direct_io, num=args.num, size=args.size)


def main() -> None:
    """Main entry point."""
    print("Running test_profile_copy_bytes")
    config, creds = _generate_rclone_config()
    print("Config:")
    print(config)
    print("Credentials:")
    print(creds)
    rclone = Rclone(config)

    mount_root_path = Path("rclone_logs") / "mount"
    if mount_root_path.exists():
        shutil.rmtree(mount_root_path)

    args = _parse_args()
    transfer_list = [args.num]
    # parallel_workers = args.num

    def task(
        offset: SizeSuffix,
        args=args,
        rclone=rclone,
        transfer_list=transfer_list,
        mount_root_path=mount_root_path,
    ):
        return test_profile_copy_bytes(
            args=args,
            rclone=rclone,
            offset=offset,
            mount_root_path=mount_root_path,
            transfer_list=transfer_list,
            size=args.size,
            num=args.num,
        )

    task(offset=SizeSuffix(0))

    # with ThreadPoolExecutor(max_workers=parallel_workers) as _:
    #     tasks: list[Future] = []
    #     for i in range(parallel_workers):
    #         offset = SizeSuffix(i * 1024 * 1024 * 256)
    #         future = ThreadPoolExecutor().submit(lambda: task(offset=offset))
    #         tasks.append(future)


if __name__ == "__main__":
    import sys

    sys.argv.append("--size")
    sys.argv.append("16MB")
    main()
