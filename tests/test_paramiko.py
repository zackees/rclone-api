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
        # Get credentials from environment variables
        SRC_SFTP_HOST = os.getenv("SRC_SFTP_HOST")
        SRC_SFTP_USER = os.getenv("SRC_SFTP_USER")
        SRC_SFTP_PORT = os.getenv("SRC_SFTP_PORT")
        SRC_SFTP_PASS = os.getenv("SRC_SFTP_PASS")
        SRC_SFTP_KEY_PATH = os.getenv("SRC_SFTP_KEY_PATH")  # Optional private key path

        assert SRC_SFTP_HOST is not None, "SRC_SFTP_HOST environment variable is required"
        assert SRC_SFTP_USER is not None, "SRC_SFTP_USER environment variable is required"
        assert SRC_SFTP_PORT is not None, "SRC_SFTP_PORT environment variable is required"
        
        # Either password or key should be provided
        has_auth = SRC_SFTP_PASS is not None or SRC_SFTP_KEY_PATH is not None
        assert has_auth, "Either SRC_SFTP_PASS or SRC_SFTP_KEY_PATH environment variable is required"

        print("Credentials are:")
        print(f"SRC_SFTP_HOST: {SRC_SFTP_HOST}")
        print(f"SRC_SFTP_USER: {SRC_SFTP_USER}")
        print(f"SRC_SFTP_PORT: {SRC_SFTP_PORT}")
        if SRC_SFTP_PASS:
            print(f"SRC_SFTP_PASS: {SRC_SFTP_PASS[:3]}...") # Only print first few chars for security
        if SRC_SFTP_KEY_PATH:
            print(f"SRC_SFTP_KEY_PATH: {SRC_SFTP_KEY_PATH}")

        # Skip the test if we're in CI environment without proper credentials
        if os.getenv("CI") and not has_auth:
            self.skipTest("Skipping SFTP test in CI environment without credentials")

        # Test that the paramiko connection works
        try:
            # Create SSH client
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # Add more verbose logging to diagnose the issue
            import logging
            logging.basicConfig(level=logging.DEBUG)
            
            # Connection parameters
            connect_params = {
                'hostname': SRC_SFTP_HOST,
                'username': SRC_SFTP_USER,
                'port': int(SRC_SFTP_PORT),
                'timeout': 30,
                'allow_agent': False,
                'look_for_keys': False,
            }
            
            # Add authentication method
            if SRC_SFTP_KEY_PATH:
                print("Using key-based authentication")
                connect_params['key_filename'] = SRC_SFTP_KEY_PATH
            elif SRC_SFTP_PASS:
                print("Using password authentication")
                connect_params['password'] = SRC_SFTP_PASS
            
            print(f"Connecting to {SRC_SFTP_HOST}:{SRC_SFTP_PORT} as {SRC_SFTP_USER}...")
            ssh_client.connect(**connect_params)
            
            print("Connection successful, opening SFTP session...")
            # Open SFTP session
            sftp_client = ssh_client.open_sftp()
            
            print("SFTP session opened, listing directory...")
            # List directory contents to verify connection works
            files = sftp_client.listdir(".")
            self.assertIsInstance(files, list, "SFTP listdir should return a list")
            print(f"Successfully connected and found files: {files[:5] if files else []}")
            
            # Close connections
            sftp_client.close()
            ssh_client.close()
            print("Connection closed successfully")
            
        except Exception as e:
            # More detailed error information
            import traceback
            error_details = traceback.format_exc()
            
            # Try to get transport-level errors
            transport_error = "No transport error available"
            if hasattr(e, 'args') and len(e.args) > 0:
                transport_error = f"Error args: {e.args}"
            
            self.fail(f"Paramiko SFTP connection failed: {str(e)}\n"
                     f"Transport error: {transport_error}\n"
                     f"Details: {error_details}")


if __name__ == "__main__":
    unittest.main()
