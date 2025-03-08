import os
import tempfile
import unittest
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from rclone_api.file_part import FilePart
from rclone_api.s3.api import S3Client
from rclone_api.s3.create import S3Provider
from rclone_api.s3.types import S3Credentials, S3MutliPartUploadConfig, S3UploadTarget
from rclone_api.s3.upload_file_multipart import MultiUploadResult

load_dotenv()


class RcloneS3Tester(unittest.TestCase):
    """Test rclone functionality."""

    # @unittest.skipIf(not _ENABLED, "Test not enabled")
    # @unittest.skipIf(True, "Test not enabled")
    @unittest.skip("Skip test")
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

        credentials = S3Credentials(
            provider=S3Provider.BACKBLAZE,
            access_key_id=ACCESS_KEY_ID,
            secret_access_key=SECRET_ACCESS_KEY,
            endpoint_url=ENDPOINT_URL,
        )

        s3_client = S3Client(credentials)

        # @dataclass
        # class S3UploadTarget:
        #     """Target information for S3 upload."""

        #     file_path: str
        #     bucket_name: str
        #     s3_key: str

        dst_path = "test_data/testfile"

        # create a file of 1MB and write binary data 0-255 cyclically.
        # s3_client = create_s3_client(credentials)

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
            state_json = Path(tempdir) / "state.json"

            def simple_fetcher(
                offset: int, chunk_size: int, extra: Any
            ) -> Future[FilePart]:
                with ThreadPoolExecutor() as executor:

                    def task(
                        tmpfile=tmpfile, offset=offset, chunk_size=chunk_size
                    ) -> FilePart:
                        with open(str(tmpfile), "rb") as f:
                            f.seek(offset)
                            data = f.read(chunk_size)
                            fp = FilePart(payload=data, extra=extra)
                            return fp

                    fut = executor.submit(task)
                return fut

            upload_target: S3UploadTarget = S3UploadTarget(
                src_file=Path(tmpfile),
                src_file_size=filesize,
                bucket_name=BUCKET_NAME,
                s3_key=dst_path,
            )

            # Uploads one chunk then stops.
            upload_config_partial: S3MutliPartUploadConfig = S3MutliPartUploadConfig(
                chunk_size=chunk_size,
                chunk_fetcher=simple_fetcher,
                max_write_threads=16,
                retries=0,
                resume_path_json=state_json,
                max_chunks_before_suspension=1,
            )

            # Finishes the upload.
            upload_config_all: S3MutliPartUploadConfig = S3MutliPartUploadConfig(
                chunk_size=chunk_size,
                chunk_fetcher=simple_fetcher,
                max_write_threads=16,
                retries=0,
                resume_path_json=state_json,
                max_chunks_before_suspension=None,
            )

            print(f"Uploading file {f.name} of size {filesize} to {BUCKET_NAME}")
            rslt: MultiUploadResult = s3_client.upload_file_multipart(
                upload_target=upload_target,
                upload_config=upload_config_partial,
            )
            self.assertEqual(rslt, MultiUploadResult.SUSPENDED)
            rslt = s3_client.upload_file_multipart(
                upload_target=upload_target,
                upload_config=upload_config_all,
            )
            self.assertEqual(rslt, MultiUploadResult.UPLOADED_RESUME)
            print(f"Uploading file {f.name} to {BUCKET_NAME}")
            state_str = state_json.read_text(encoding="utf-8")
            print("Finished state:\n", state_str)
            print("Upload successful.")
            print(f"Uploading file {f.name} to {BUCKET_NAME}/{dst_path}")
            rslt = s3_client.upload_file_multipart(
                upload_target=upload_target,
                upload_config=upload_config_all,
            )
            self.assertEqual(rslt, MultiUploadResult.ALREADY_DONE)
            print("done")


if __name__ == "__main__":
    unittest.main()
