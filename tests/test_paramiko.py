"""
Unit test file.
"""

import os
import unittest

import paramiko
from dotenv import load_dotenv

load_dotenv()


class RcloneLsTests(unittest.TestCase):
    """Test rclone functionality."""

    def test_sftp_resumable_file_copy_to_s3(self) -> None:

        SRC_SFTP_HOST = os.getenv("SRC_SFTP_HOST")
        SRC_SFTP_USER = os.getenv("SRC_SFTP_USER")
        SRC_SFTP_PORT = os.getenv("SRC_SFTP_PORT")
        SRC_SFTP_PASS = os.getenv("SRC_SFTP_PASS")

        assert SRC_SFTP_HOST is not None
        assert SRC_SFTP_USER is not None
        assert SRC_SFTP_PORT is not None
        assert SRC_SFTP_PASS is not None

        print("Credentials are:")
        print(f"SRC_SFTP_HOST: {SRC_SFTP_HOST}")
        print(f"SRC_SFTP_USER: {SRC_SFTP_USER}")
        print(f"SRC_SFTP_PORT: {SRC_SFTP_PORT}")
        print(f"SRC_SFTP_PASS: {SRC_SFTP_PASS}")

        # test that the paramiko connection works
        try:
            # Create SSH client
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            # Connect to SFTP server
            ssh_client.connect(
                hostname=SRC_SFTP_HOST,
                username=SRC_SFTP_USER,
                password=SRC_SFTP_PASS,
                port=int(SRC_SFTP_PORT),
            )

            # Open SFTP session
            sftp_client = ssh_client.open_sftp()

            # List directory contents to verify connection works
            files = sftp_client.listdir(".")
            self.assertIsInstance(files, list, "SFTP listdir should return a list")

            # Close connections
            sftp_client.close()
            ssh_client.close()

        except Exception as e:
            self.fail(f"Paramiko SFTP connection failed: {str(e)}")


if __name__ == "__main__":
    unittest.main()
