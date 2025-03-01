"""
Unit test file.
"""

import os
import time
from dataclasses import dataclass
from pathlib import Path

import psutil
from dotenv import load_dotenv

from rclone_api import Config, Rclone, SizeSuffix

load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")  # Default if not in .env


@dataclass
class Credentials:
    BUCKET_KEY_SECRET: str
    BUCKET_KEY_PUBLIC: str
    SRC_SFTP_HOST: str
    SRC_SFTP_USER: str
    SRC_SFTP_PORT: str
    SRC_SFTP_PASS: str
    BUCKET_URL: str


def _generate_rclone_config() -> tuple[Config, Credentials]:

    cwd = Path.cwd()
    print(f"Current working directory: {cwd}")

    # assert that .env exists for this test
    assert os.path.exists(
        ".env"
    ), "this test requires that the secret .env file exists with the credentials"

    # BUCKET_NAME = os.getenv("BUCKET_NAME", "TorrentBooks")  # Default if not in .env

    # Load additional environment variables
    BUCKET_KEY_SECRET = os.getenv("BUCKET_KEY_SECRET")
    BUCKET_KEY_PUBLIC = os.getenv("BUCKET_KEY_PUBLIC")
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
    print("Config text:")
    print(config_text)
    # _CONFIG_PATH.write_text(config_text, encoding="utf-8")
    # print(f"Config file written to: {_CONFIG_PATH}")

    creds = Credentials(
        BUCKET_KEY_SECRET=str(BUCKET_KEY_SECRET),
        BUCKET_KEY_PUBLIC=str(BUCKET_KEY_PUBLIC),
        SRC_SFTP_HOST=str(SRC_SFTP_HOST),
        SRC_SFTP_USER=str(SRC_SFTP_USER),
        SRC_SFTP_PORT=str(SRC_SFTP_PORT),
        SRC_SFTP_PASS=str(SRC_SFTP_PASS),
        BUCKET_URL=str(BUCKET_URL),
    )

    return Config(config_text), creds


def _init() -> None:
    """Check if all required environment variables are set before running tests."""
    required_vars = [
        "BUCKET_NAME",
        "BUCKET_KEY_SECRET",
        "BUCKET_KEY_PUBLIC",
        "BUCKET_URL",
    ]
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        print(f"Missing required environment variables: {', '.join(missing)}")
    os.environ["RCLONE_API_VERBOSE"] = "1"


def test_profile_copy_bytes() -> None:
    print("Running test_profile_copy_bytes")
    config, creds = _generate_rclone_config()
    print("Config:")
    print(config)
    print("Credentials:")
    print(creds)
    rclone = Rclone(config)

    sizes = [
        1024 * 1024 * 1,
        1024 * 1024 * 2,
        1024 * 1024 * 4,
        1024 * 1024 * 8,
        1024 * 1024 * 16,
        1024 * 1024 * 32,
        1024 * 1024 * 64,
    ]
    # transfer_list = [1, 2, 4, 8, 16]
    transfer_list = [1, 2, 4]

    # src_file = "dst:rclone-api-unit-test/zachs_video/internaly_ai_alignment.mp4"
    # sftp mount
    src_file = "src:aa_misc_data/aa_misc_data/world_lending_library_2024_11.tar.zst"

    for size in sizes:
        for transfers in transfer_list:
            mount_log = Path("logs") / "mount" / f"mount_{size}_{transfers}.log"
            print("\n\n")
            print("#" * 80)
            print(
                f"# Started test download of {SizeSuffix(size)} with {transfers} transfers"
            )
            print("#" * 80)
            net_io_start = psutil.net_io_counters()
            start = time.time()
            bytes_or_err: bytes | Exception = rclone.copy_bytes(
                src=src_file,
                offset=0,
                length=size,
                direct_io=True,
                transfers=transfers,
                mount_log=mount_log,
            )
            diff = time.time() - start
            net_io_end = psutil.net_io_counters()
            if isinstance(bytes_or_err, Exception):
                print(bytes_or_err)
                stack_trace = bytes_or_err.__traceback__
                assert False, f"Error: {bytes_or_err}\nStack trace:\n{stack_trace}"
            assert isinstance(bytes_or_err, bytes)
            # self.assertEqual(len(bytes_or_err), size)
            assert len(bytes_or_err) == size, f"Length: {len(bytes_or_err)} != {size}"

            # print io stats
            bytes_sent = net_io_end.bytes_sent - net_io_start.bytes_sent
            bytes_recv = net_io_end.bytes_recv - net_io_start.bytes_recv
            packets_sent = net_io_end.packets_sent - net_io_start.packets_sent
            efficiency = size / (bytes_recv)
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

    print("done")


def main() -> None:
    """Main entry point."""
    _init()
    test_profile_copy_bytes()


if __name__ == "__main__":
    main()
