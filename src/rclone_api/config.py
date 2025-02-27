from dataclasses import dataclass, field
from typing import Dict, List


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
