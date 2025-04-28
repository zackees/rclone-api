import atexit
import subprocess
import threading
import weakref
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psutil

from rclone_api.config import Config
from rclone_api.util import clear_temp_config_file, get_verbose, make_temp_config_file


@dataclass
class ProcessArgs:
    cmd: list[str]
    rclone_conf: Path | Config | None
    rclone_exe: Path
    cmd_list: list[str]
    verbose: bool | None = None
    capture_stdout: bool | None = None
    log: Path | None = None


class Process:
    def __init__(self, args: ProcessArgs) -> None:
        assert (
            args.rclone_exe.exists()
        ), f"rclone executable not found: {args.rclone_exe}"
        self.args = args
        self.log = args.log
        self.cleaned_up = False
        self.tempfile: Path | None = None
        rclone_conf: Path | None = None
        verbose = get_verbose(args.verbose)
        # Create a temporary config file if needed.
        if isinstance(args.rclone_conf, Config):
            self.tempfile = make_temp_config_file()
            self.tempfile.write_text(args.rclone_conf.text, encoding="utf-8")
            rclone_conf = self.tempfile
        else:
            rclone_conf = args.rclone_conf
        # assert rclone_conf.exists(), f"rclone config not found: {rclone_conf}"
        # Build the command.
        self.cmd = [str(args.rclone_exe.resolve())]
        if rclone_conf:
            self.cmd += ["--config", str(rclone_conf.resolve())]
        self.cmd += args.cmd_list
        if self.args.log:
            self.args.log.parent.mkdir(parents=True, exist_ok=True)
            self.cmd += ["--log-file", str(self.args.log)]
        if verbose:
            cmd_str = subprocess.list2cmdline(self.cmd)
            print(f"Running: {cmd_str}")
        kwargs: dict = {"shell": False}
        if args.capture_stdout:
            kwargs["stdout"] = subprocess.PIPE
            kwargs["stderr"] = subprocess.STDOUT

        self.process = subprocess.Popen(self.cmd, **kwargs)  # type: ignore

        # Register an atexit callback using a weak reference to avoid keeping the Process instance alive.
        self_ref = weakref.ref(self)

        def exit_cleanup():
            obj = self_ref()
            if obj is not None:
                obj._atexit_terminate()

        atexit.register(exit_cleanup)

    def __enter__(self) -> "Process":
        return self

    def dispose(self) -> None:
        if self.cleaned_up:
            return
        self.cleaned_up = True
        self.terminate()
        self.wait()
        self.cleanup()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.dispose()

    def cleanup(self) -> None:
        if self.tempfile:
            clear_temp_config_file(self.tempfile)

    def _kill_process_tree(self) -> None:
        """
        Use psutil to recursively terminate the main process and all its child processes.
        """
        try:
            parent = psutil.Process(self.process.pid)
        except psutil.NoSuchProcess:
            return

        # Terminate child processes.
        children = parent.children(recursive=True)
        if children:
            print(f"Terminating {len(children)} child processes...")
            for child in children:
                try:
                    child.terminate()
                except Exception as e:
                    print(f"Error terminating child process {child.pid}: {e}")
            psutil.wait_procs(children, timeout=2)
            # Kill any that remain.
            for child in children:
                if child.is_running():
                    try:
                        child.kill()
                    except Exception as e:
                        print(f"Error killing child process {child.pid}: {e}")

        # Terminate the parent process.
        if parent.is_running():
            try:
                parent.terminate()
            except Exception as e:
                print(f"Error terminating process {parent.pid}: {e}")
            try:
                parent.wait(timeout=3)
            except psutil.TimeoutExpired:
                try:
                    parent.kill()
                except Exception as e:
                    print(f"Error killing process {parent.pid}: {e}")

    def _atexit_terminate(self) -> None:
        """
        This method is registered via atexit and uses psutil to clean up the process tree.
        It runs in a daemon thread so that termination happens without blocking interpreter shutdown.
        """
        if self.process.poll() is None:  # Process is still running.

            def terminate_sequence():
                self._kill_process_tree()

            t = threading.Thread(target=terminate_sequence, daemon=True)
            t.start()
            t.join(timeout=3)

    @property
    def pid(self) -> int:
        return self.process.pid

    def __del__(self) -> None:
        self.cleanup()

    def kill(self) -> None:
        """Forcefully kill the process tree."""
        try:
            self._kill_process_tree()
        except Exception as e:
            print(f"Error killing process tree: {e}")

    def terminate(self) -> None:
        """Gracefully terminate the process tree."""
        self._kill_process_tree()

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

    def send_signal(self, sig: int) -> None:
        self.process.send_signal(sig)

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
