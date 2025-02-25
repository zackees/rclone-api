from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class ParsedSection:
    name: str
    options: Dict[str, str] = field(default_factory=dict)


@dataclass
class Parsed:
    sections: List[ParsedSection]

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
    sections: List[ParsedSection] = []
    current_section: ParsedSection | None = None

    lines = content.splitlines()
    for line in lines:
        line = line.strip()
        # Skip empty lines and comments (assumed to start with '#' or ';')
        if not line or line.startswith(("#", ";")):
            continue
        # New section header detected
        if line.startswith("[") and line.endswith("]"):
            section_name = line[1:-1].strip()
            current_section = ParsedSection(name=section_name)
            sections.append(current_section)
        elif "=" in line and current_section is not None:
            # Parse key and value, splitting only on the first '=' found
            key, value = line.split("=", 1)
            current_section.options[key.strip()] = value.strip()

    # return sections
    return Parsed(sections=sections)
