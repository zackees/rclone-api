from rclone_api.file import File


def convert_to_filestr_list(files: str | File | list[str] | list[File]) -> list[str]:
    out: list[str] = []
    if isinstance(files, str):
        out.append(files)
    elif isinstance(files, File):
        out.append(str(files.path))
    elif isinstance(files, list):
        for f in files:
            if isinstance(f, File):
                f = str(f.path)
            out.append(f)
    else:
        raise ValueError(f"Invalid type for file: {type(files)}")
    return out
