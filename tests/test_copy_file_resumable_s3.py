import os
import unittest

# context lib
from pathlib import Path

from dotenv import load_dotenv

from rclone_api import PartInfo, Range, Rclone, SizeSuffix

_HERE = Path(__file__).parent
_PROJECT_ROOT = _HERE.parent
_CONFIG_PATH = _PROJECT_ROOT / "rclone-mounted-ranged-download.conf"

_CHUNK_SIZE = 1024 * 1024 * 16

_CHUNK_SIZE *= 10

load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")  # Default if not in .env


def _generate_rclone_config(port: int) -> str:

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


[webdav]
type = webdav
user = guest
# obscured password for "1234", use Rclone.obscure("1234") to generate
pass = d4IbQLV9W0JhI2tm5Zp88hpMtEg
url = http://localhost:{port}
vendor = rclone
"""
    # _CONFIG_PATH.write_text(config_text, encoding="utf-8")
    # print(f"Config file written to: {_CONFIG_PATH}")
    return config_text


PORT = 8095


class RcloneCopyResumableFileToS3(unittest.TestCase):
    """Test rclone functionality."""

    def setUp(self) -> None:
        """Check if all required environment variables are set before running tests."""
        required_vars = [
            "BUCKET_NAME",
            "BUCKET_KEY_SECRET",
            "BUCKET_KEY_PUBLIC",
            "BUCKET_URL",
        ]
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            self.skipTest(
                f"Missing required environment variables: {', '.join(missing)}"
            )
        os.environ["RCLONE_API_VERBOSE"] = "1"

    @unittest.skip("Skip for now - long running test")
    def test_upload_chunks(self) -> None:
        """Test basic Webdav serve functionality."""
        # config = _generate_rclone_config(PORT)
        BUCKET_NAME: str | None = os.getenv("B2_BUCKET_NAME")
        ACCESS_KEY_ID: str | None = os.getenv("B2_ACCESS_KEY_ID")
        SECRET_ACCESS_KEY: str | None = os.getenv("B2_SECRET_ACCESS_KEY")
        ENDPOINT_URL: str | None = os.getenv("B2_ENDPOINT_URL")
        assert BUCKET_NAME
        assert ACCESS_KEY_ID
        assert SECRET_ACCESS_KEY
        assert ENDPOINT_URL
        print(f"BUCKET_KEY_SECRET: {SECRET_ACCESS_KEY}")
        config_text = _generate_rclone_config(PORT)
        _CONFIG_PATH.write_text(config_text, encoding="utf-8")
        print(f"Config file written to: {_CONFIG_PATH}")
        rclone = Rclone(_CONFIG_PATH)
        save_state_json = Path("state.json")
        if save_state_json.exists():
            save_state_json.unlink()
        rclone.copy_file_resumable_s3(
            src="dst:rclone-api-unit-test/zachs_video/perpetualmaniac_an_authoritative_school_teacher_forcing_a_stude_c65528d3-aa6f-4777-a56b-a919856d44e1.png",
            dst="dst:rclone-api-unit-test/test_data/test.torrent.testwrite",
            chunk_size=SizeSuffix("16MB"),
            retries=0,
            save_state_json=save_state_json,
            max_chunks_before_suspension=1,
        )
        print("Done")

    def test_copy_parts(self) -> None:
        src_file = "dst:rclone-api-unit-test/zachs_video/perpetualmaniac_an_authoritative_school_teacher_forcing_a_stude_c65528d3-aa6f-4777-a56b-a919856d44e1.png"
        dst_dir = "dst:rclone-api-unit-test/test_data/test2.png-parts/"

        # print(f"BUCKET_KEY_SECRET: {SECRET_ACCESS_KEY}")
        config_text = _generate_rclone_config(PORT)
        _CONFIG_PATH.write_text(config_text, encoding="utf-8")
        print(f"Config file written to: {_CONFIG_PATH}")
        rclone = Rclone(_CONFIG_PATH)

        src_size: SizeSuffix | Exception = rclone.impl.size_file(src_file)
        assert isinstance(src_size, SizeSuffix)

        print(f"src_size: {src_size}")

        # now break it up into 10 parts
        chunk_size = src_size / 10

        part_infos: list[PartInfo] = []
        curr_offset: int = 0
        part_number: int = 0
        while True:
            part_number += 1
            done = False
            end = curr_offset + chunk_size
            if end > src_size:
                done = True
                chunk_size = src_size - curr_offset
            range: Range = Range(start=curr_offset, end=curr_offset + chunk_size)
            part_info = PartInfo(
                part_number=part_number,
                range=range,
            )
            part_infos.append(part_info)
            curr_offset += chunk_size.as_int()
            if curr_offset >= src_size:
                break
            if done:
                break

        rclone.impl.copy_file_parts(
            src=src_file,
            dst_dir=dst_dir,
            part_infos=part_infos,
        )

        dir_listing = rclone.ls(dst_dir)
        print(f"dir_listing: {dir_listing}")

        rclone.purge(dst_dir)
        print("Done")


if __name__ == "__main__":
    unittest.main()
