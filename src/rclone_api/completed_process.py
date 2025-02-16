import subprocess
from dataclasses import dataclass


@dataclass
class CompletedProcess:
    completed: list[subprocess.CompletedProcess]

    @property
    def ok(self) -> bool:
        return all([p.returncode == 0 for p in self.completed])

    @staticmethod
    def from_subprocess(process: subprocess.CompletedProcess) -> "CompletedProcess":
        return CompletedProcess(completed=[process])

    def failed(self) -> list[subprocess.CompletedProcess]:
        return [p for p in self.completed if p.returncode != 0]

    def successes(self) -> list[subprocess.CompletedProcess]:
        return [p for p in self.completed if p.returncode == 0]
