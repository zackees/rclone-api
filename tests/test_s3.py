import os
import tempfile
import unittest

# context lib
from pathlib import Path

from dotenv import load_dotenv

from rclone_api.s3_chunk_uploader import MultiUploadResult, upload_file_multipart
from rclone_api.s3_create_client import (
    create_backblaze_s3_client,
)

_IS_WINDOWS = os.name == "nt"

_ENABLED = not _IS_WINDOWS
_CHUNK_SIZE = 1024 * 1024 * 3

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

        chunk_size = 5 * 1024 * 1024
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
            tmpstate = Path(tempdir) / "state.json"
            print(f"Uploading file {f.name} of size {filesize} to {BUCKET_NAME}")
            rslt: MultiUploadResult = upload_file_multipart(
                s3_client=s3_client,
                bucket_name=BUCKET_NAME,
                file_path=f.name,
                resumable_info_path=tmpstate,
                object_name="testfile",
                chunk_size=chunk_size,
                retries=0,
                max_chunks_before_suspension=1,
            )
            self.assertEqual(rslt, MultiUploadResult.SUSPENDED)
            rslt = upload_file_multipart(
                s3_client=s3_client,
                bucket_name=BUCKET_NAME,
                file_path=f.name,
                resumable_info_path=tmpstate,
                object_name="testfile",
                chunk_size=chunk_size,
                retries=0,
            )
            self.assertEqual(rslt, MultiUploadResult.UPLOADED_RESUME)
            print(f"Uploading file {f.name} to {BUCKET_NAME}")
            state_str = tmpstate.read_text(encoding="utf-8")
            print("Finished state:\n", state_str)
            print("Upload successful.")
            print(f"Uploading file {f.name} to {BUCKET_NAME}")
            rslt = upload_file_multipart(
                s3_client=s3_client,
                bucket_name=BUCKET_NAME,
                file_path=f.name,
                resumable_info_path=tmpstate,
                object_name="testfile",
                chunk_size=chunk_size,
                retries=0,
            )
            self.assertEqual(rslt, MultiUploadResult.ALREADY_DONE)
            print("done")


if __name__ == "__main__":
    unittest.main()
