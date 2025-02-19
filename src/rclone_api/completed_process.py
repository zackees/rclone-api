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

    @property
    def stdout(self) -> str:
        tmp: list[str] = []
        for cp in self.completed:
            stdout = cp.stdout
            if stdout is not None:
                tmp.append(stdout)
        return "\n".join(tmp)

    @property
    def stderr(self) -> str:
        tmp: list[str] = []
        for cp in self.completed:
            stderr = cp.stderr
            if stderr is not None:
                tmp.append(stderr)
        return "\n".join(tmp)

    @property
    def returncode(self) -> int | None:
        for cp in self.completed:
            rtn = cp.returncode
            if rtn is None:
                return None
            if rtn != 0:
                return rtn
        return 0

    def __str__(self) -> str:

        cmd_strs: list[str] = []
        rtn_cods: list[int] = []
        for cp in self.completed:
            cmd_strs.append(subprocess.list2cmdline(cp.args))
            rtn_cods.append(cp.returncode)
        msg = f"CompletedProcess: {len(cmd_strs)} commands\n"
        msg += "\n".join([f"{cmd} -> {rtn}" for cmd, rtn in zip(cmd_strs, rtn_cods)])
        return msg
