import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List


@dataclass
class Section:
    name: str
    data: Dict[str, str] = field(default_factory=dict)

    def add(self, key: str, value: str) -> None:
        self.data[key] = value

    def type(self) -> str:
        return self.data["type"]

    def provider(self) -> str | None:
        return self.data.get("provider")

    def access_key_id(self) -> str:
        if "access_key_id" in self.data:
            return self.data["access_key_id"]
        elif "account" in self.data:
            return self.data["account"]
        raise KeyError("No access key found")

    def secret_access_key(self) -> str:
        # return self.data["secret_access_key"]
        if "secret_access_key" in self.data:
            return self.data["secret_access_key"]
        elif "key" in self.data:
            return self.data["key"]
        raise KeyError("No secret access key found")

    def endpoint(self) -> str | None:
        return self.data.get("endpoint")


@dataclass
class Parsed:
    # sections: List[ParsedSection]
    sections: dict[str, Section]

    @staticmethod
    def parse(content: str) -> "Parsed":
        return parse_rclone_config(content)


@dataclass
class Config:
    """Rclone configuration dataclass."""

    text: str

    def parse(self) -> Parsed:
        return Parsed.parse(self.text)


def find_conf_file(rclone: Any | None = None) -> Path | None:
    import subprocess

    from rclone_api import Rclone
    from rclone_api.rclone_impl import RcloneImpl

    # if os.environ.get("RCLONE_CONFIG"):
    #     return Path(os.environ["RCLONE_CONFIG"])
    # return None
    # rclone_conf = rclone_conf or Path.cwd() / "rclone.conf"

    if os.environ.get("RCLONE_CONFIG"):
        return Path(os.environ["RCLONE_CONFIG"])
    if (conf := Path.cwd() / "rclone.conf").exists():
        return conf

    if rclone is None:
        from rclone_api.install import rclone_download

        err = rclone_download(Path("."))
        if isinstance(err, Exception):
            import warnings

            warnings.warn(f"rclone_download failed: {err}")
            return None
        cmd_list: list[str] = [
            "rclone",
            "config",
            "paths",
        ]
        subproc: subprocess.CompletedProcess = subprocess.run(
            args=cmd_list,
            shell=True,
            capture_output=True,
            text=True,
        )
        if subproc.returncode == 0:
            stdout = subproc.stdout
            for line in stdout.splitlines():
                parts = line.split(":", 1)
                if len(parts) == 2:
                    _, value = parts
                    value = value.strip()
                    value_path = Path(value.strip())
                    if value_path.exists():
                        return value_path
    else:
        if isinstance(rclone, Rclone):
            rclone = rclone.impl
        else:
            assert isinstance(rclone, RcloneImpl)
        rclone_impl: RcloneImpl = rclone
        assert isinstance(rclone_impl, RcloneImpl)
        paths_or_err = rclone_impl.config_paths()
        if isinstance(paths_or_err, Exception):
            return None
        paths = paths_or_err
        path: Path
        for path in paths:
            if path.exists():
                return path
    return None


def parse_rclone_config(content: str) -> Parsed:
    """
    Parses an rclone configuration file and returns a list of RcloneConfigSection objects.

    Each section in the file starts with a line like [section_name]
    followed by key=value pairs.
    """
    sections: List[Section] = []
    current_section: Section | None = None

    lines = content.splitlines()
    for line in lines:
        line = line.strip()
        # Skip empty lines and comments (assumed to start with '#' or ';')
        if not line or line.startswith(("#", ";")):
            continue
        # New section header detected
        if line.startswith("[") and line.endswith("]"):
            section_name = line[1:-1].strip()
            current_section = Section(name=section_name)
            sections.append(current_section)
        elif "=" in line and current_section is not None:
            # Parse key and value, splitting only on the first '=' found
            key, value = line.split("=", 1)
            current_section.add(key.strip(), value.strip())

    data: dict[str, Section] = {}
    for section in sections:
        data[section.name] = section
    return Parsed(sections=data)
