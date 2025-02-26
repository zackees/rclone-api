import os
import unittest

# context lib
from pathlib import Path

from dotenv import load_dotenv

from rclone_api.s3_chunk_uploader import upload_file_multipart
from rclone_api.s3_create_client import (
    create_backblaze_s3_client,
    upload_file,
)

_IS_WINDOWS = os.name == "nt"

_ENABLED = not _IS_WINDOWS
_CHUNK_SIZE = 1024 * 1024 * 16

_CHUNK_SIZE *= 10

load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")  # Default if not in .env


class RcloneS3Tester(unittest.TestCase):
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

    # @unittest.skipIf(not _ENABLED, "Test not enabled")
    # @unittest.skipIf(True, "Test not enabled")
    def test_upload_chunks(self) -> None:
        """Test basic Webdav serve functionality."""
        # config = _generate_rclone_config(PORT)
        # print("AWS CREDENTIALS")
        # print(f"BUCKET_NAME: {BUCKET_NAME}")
        # BUCKET_KEY_SECRET = os.getenv("BUCKET_KEY_SECRET")
        # BUCKET_KEY_PUBLIC = os.getenv("BUCKET_KEY_PUBLIC")
        # ENDPOINT_URL = "https://s3.us-west-002.backblazeb2.com"
        # print(f"BUCKET_KEY_PUBLIC: {BUCKET_KEY_PUBLIC}")
        # print(f"BUCKET_KEY_SECRET: {BUCKET_KEY_SECRET}")

        # B2_BUCKET_NAME=TorrentBooks
        # B2_ACCESS_KEY_ID=002829ba08b64e7000000002c
        # B2_SECRET_ACCESS_KEY=K0029aIZv/VJGWqTEt8YWB77WI9lkc8
        # B2_ENDPOINT_URL=https://s3.us-west-002.backblazeb2.com

        # BUCKET_NAME: str = "TorrentBooks"
        # ACCESS_KEY_ID: str = "002829ba08b64e7000000002c"
        # SECRET_ACCESS_KEY: str = "K0029aIZv/VJGWqTEt8YWB77WI9lkc8"
        # ENDPOINT_URL: str = "https://s3.us-west-002.backblazeb2.com"

        BUCKET_NAME: str | None = os.getenv("B2_BUCKET_NAME")
        ACCESS_KEY_ID: str | None = os.getenv("B2_ACCESS_KEY_ID")
        SECRET_ACCESS_KEY: str | None = os.getenv("B2_SECRET_ACCESS_KEY")
        ENDPOINT_URL: str | None = os.getenv("B2_ENDPOINT_URL")
        assert BUCKET_NAME
        assert ACCESS_KEY_ID
        assert SECRET_ACCESS_KEY
        assert ENDPOINT_URL

        # create a file of 1MB and write binary data 0-255 cyclically.
        s3_client = create_backblaze_s3_client(
            access_key=ACCESS_KEY_ID,
            secret_key=SECRET_ACCESS_KEY,
            endpoint_url=ENDPOINT_URL,
        )

        # list_bucket_contents(s3_client, BUCKET_NAME)
        import tempfile

        # numbers_0_255 = bytearray(range(256))
        pattern = bytes(range(256))
        _bytes = bytearray(pattern * (16 * 1024 * 1024 // len(pattern)))

        print("Payload size: ", len(_bytes))

        with tempfile.TemporaryDirectory() as tempdir:
            tmpfile = Path(tempdir) / "testfile"
            with open(str(tmpfile), "wb") as f:
                f.write(_bytes)  # this will create a 1MB file of 0-255 cyclically.
                f.flush()
                f.seek(0)

            filesize = tmpfile.stat().st_size

            print(f"Uploading file {f.name} of size {filesize} to {BUCKET_NAME}")
            upload_file_multipart(s3_client, BUCKET_NAME, f.name, "testfile", retries=0)
            err = upload_file(
                s3_client,
                BUCKET_NAME,
                f.name,
                f"{BUCKET_NAME}/test/testfile",
            )
            if err:
                raise Exception(err)


if __name__ == "__main__":
    unittest.main()
