# rclone-api

[![Linting](https://github.com/zackees/rclone-api/actions/workflows/lint.yml/badge.svg)](https://github.com/zackees/rclone-api/actions/workflows/lint.yml)
[![MacOS_Tests](https://github.com/zackees/rclone-api/actions/workflows/push_macos.yml/badge.svg)](https://github.com/zackees/rclone-api/actions/workflows/push_macos.yml)
[![Ubuntu_Tests](https://github.com/zackees/rclone-api/actions/workflows/push_ubuntu.yml/badge.svg)](https://github.com/zackees/rclone-api/actions/workflows/push_ubuntu.yml)
[![Win_Tests](https://github.com/zackees/rclone-api/actions/workflows/push_win.yml/badge.svg)](https://github.com/zackees/rclone-api/actions/workflows/push_win.yml)

Api version of rclone. It's well tested. It's a pretty low level api without the bells and whistles of other apis, but it will get the job done.

You will need to have rclone installed and on your path.

One of the benefits of this api is that it does not use `shell=True`, which can keep `rclone` running in some instances even when try to kill the process.

# Install

`pip install rclone-api`


# Examples

You can use env variables or use a `.env` file to store your secrets.


# Rclone API Usage Examples

This script demonstrates how to interact with DigitalOcean Spaces using `rclone_api`. 

## Setup & Usage

Ensure you have set the required environment variables:

- `BUCKET_NAME`
- `BUCKET_KEY_PUBLIC`
- `BUCKET_KEY_SECRET`
- `BUCKET_URL`

Then, run the following Python script:

```python
import os
from rclone_api import Config, DirListing, File, Rclone, Remote

# Load environment variables
BUCKET_NAME = os.getenv("BUCKET_NAME")
BUCKET_KEY_PUBLIC = os.getenv("BUCKET_KEY_PUBLIC")
BUCKET_KEY_SECRET = os.getenv("BUCKET_KEY_SECRET")
BUCKET_URL = "sfo3.digitaloceanspaces.com"

# Generate Rclone Configuration
def generate_rclone_config() -> Config:
    config_text = f"""
    [dst]
    type = s3
    provider = DigitalOcean
    access_key_id = {BUCKET_KEY_PUBLIC}
    secret_access_key = {BUCKET_KEY_SECRET}
    endpoint = {BUCKET_URL}
    """
    return Config(config_text)

rclone = Rclone(generate_rclone_config())

# List Available Remotes
print("\n=== Available Remotes ===")
remotes = rclone.listremotes()
for remote in remotes:
    print(remote)

# List Contents of the Root Bucket
print("\n=== Listing Root Bucket ===")
listing = rclone.ls(f"dst:{BUCKET_NAME}", max_depth=-1)

print("\nDirectories:")
for dir in listing.dirs:
    print(dir)

print("\nFiles:")
for file in listing.files:
    print(file)

# List a Specific Subdirectory
print("\n=== Listing 'zachs_video' Subdirectory ===")
path = f"dst:{BUCKET_NAME}/zachs_video"
listing = rclone.ls(path)
print(listing)

# List PNG Files in a Subdirectory
print("\n=== Listing PNG Files ===")
listing = rclone.ls(path, glob="*.png")

if listing.files:
    for file in listing.files:
        print(file)

# Copy a File
print("\n=== Copying a File ===")
if listing.files:
    file = listing.files[0]
    new_path = f"dst:{BUCKET_NAME}/zachs_video/{file.name}_copy"
    rclone.copyfile(file, new_path)
    print(f"Copied {file.name} to {new_path}")

# Copy Multiple Files
print("\n=== Copying Multiple Files ===")
if listing.files:
    file_mapping = {file.name: file.name + "_copy" for file in listing.files[:2]}
    rclone.copyfiles(file_mapping)
    print(f"Copied files: {file_mapping}")

# Delete a File
print("\n=== Deleting a File ===")
file_to_delete = f"dst:{BUCKET_NAME}/zachs_video/sample.png_copy"
rclone.deletefiles([file_to_delete])
print(f"Deleted {file_to_delete}")

# Walk Through a Directory
print("\n=== Walking Through a Directory ===")
for dirlisting in rclone.walk(f"dst:{BUCKET_NAME}", max_depth=1):
    print(dirlisting)

print("Done.")
```


To develop software, run `. ./activate`

# Windows

This environment requires you to use `git-bash`.

# Linting

Run `./lint`
