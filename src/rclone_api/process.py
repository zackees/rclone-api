import atexit
import subprocess
import threading
import time
import weakref
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from rclone_api.config import Config
from rclone_api.util import get_verbose


@dataclass
class ProcessArgs:
    cmd: list[str]
    rclone_conf: Path | Config
    rclone_exe: Path
    cmd_list: list[str]
    verbose: bool | None = None
    capture_stdout: bool | None = None
    log: Path | None = None


class Process:
    def __init__(self, args: ProcessArgs) -> None:
        assert args.rclone_exe.exists()
        self.args = args
        self.log = args.log
        self.tempdir: TemporaryDirectory | None = None
        verbose = get_verbose(args.verbose)
        if isinstance(args.rclone_conf, Config):
            self.tempdir = TemporaryDirectory()
            tmpfile = Path(self.tempdir.name) / "rclone.conf"
            tmpfile.write_text(args.rclone_conf.text, encoding="utf-8")
            rclone_conf = tmpfile
            self.needs_cleanup = True
        else:
            rclone_conf = args.rclone_conf
            self.needs_cleanup = False

        assert rclone_conf.exists()

        self.cmd = (
            [str(args.rclone_exe.resolve())]
            + ["--config", str(rclone_conf.resolve())]
            + args.cmd
        )
        if self.args.log:
            self.args.log.parent.mkdir(parents=True, exist_ok=True)
            self.cmd += ["--log-file", str(self.args.log)]
        if verbose:
            cmd_str = subprocess.list2cmdline(self.cmd)
            print(f"Running: {cmd_str}")
        kwargs: dict = {}
        kwargs["shell"] = False
        if args.capture_stdout:
            kwargs["stdout"] = subprocess.PIPE
            kwargs["stderr"] = subprocess.STDOUT

        self.process = subprocess.Popen(self.cmd, **kwargs)  # type: ignore

        # Register an atexit callback using a weak reference to avoid
        # keeping the Process instance alive solely due to the callback.
        self_ref = weakref.ref(self)

        def exit_cleanup():
            obj = self_ref()
            if obj is not None:
                obj._atexit_terminate()

        atexit.register(exit_cleanup)

    def __enter__(self) -> "Process":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.terminate()
        self.wait()
        self.cleanup()

    def cleanup(self) -> None:
        if self.tempdir and self.needs_cleanup:
            try:
                self.tempdir.cleanup()
            except Exception as e:
                print(f"Error cleaning up tempdir: {e}")

    def _atexit_terminate(self) -> None:
        """
        Registered via atexit, this method attempts to gracefully terminate the process.
        If the process does not exit within a short timeout, it is aggressively killed.
        """
        if self.process.poll() is None:  # Process is still running

            def terminate_sequence():
                try:
                    # Try to terminate gracefully.
                    self.process.terminate()
                except Exception as e:
                    print(f"Error calling terminate on process {self.process.pid}: {e}")
                # Allow time for graceful shutdown.
                timeout = 2  # seconds
                start = time.time()
                while self.process.poll() is None and (time.time() - start) < timeout:
                    time.sleep(0.1)
                # If still running, kill aggressively.
                if self.process.poll() is None:
                    try:
                        self.process.kill()
                    except Exception as e:
                        print(f"Error calling kill on process {self.process.pid}: {e}")
                # Optionally wait briefly for termination.
                try:
                    self.process.wait(timeout=1)
                except Exception:
                    pass

            # Run the termination sequence in a separate daemon thread.
            t = threading.Thread(target=terminate_sequence, daemon=True)
            t.start()
            t.join(timeout=3)

    @property
    def pid(self) -> int:
        return self.process.pid

    def __del__(self) -> None:
        self.cleanup()

    def kill(self) -> None:
        self.cleanup()
        return self.process.kill()

    def terminate(self) -> None:
        self.cleanup()
        return self.process.terminate()

    @property
    def returncode(self) -> int | None:
        return self.process.returncode

    @property
    def stdout(self) -> Any:
        return self.process.stdout

    @property
    def stderr(self) -> Any:
        return self.process.stderr

    def poll(self) -> int | None:
        return self.process.poll()

    def wait(self) -> int:
        return self.process.wait()

    def send_signal(self, signal: int) -> None:
        return self.process.send_signal(signal)

    def __str__(self) -> str:
        state = ""
        rtn = self.process.poll()
        if rtn is None:
            state = "running"
        elif rtn != 0:
            state = f"error: {rtn}"
        else:
            state = "finished ok"
        return f"Process({self.cmd}, {state})"
