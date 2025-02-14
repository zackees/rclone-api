from dataclasses import dataclass
from enum import Enum
from queue import Queue

from rclone_api.process import Process


class LineType(Enum):
    EQUAL = 1
    MISSING_ON_SRC = 2
    MISSING_ON_DST = 3


@dataclass
class QueueItem:
    line_type: LineType
    line: str


def process_output_to_diff_stream(
    running_process: Process,
    src_slug: str,
    dst_slug: str,
    output: Queue[QueueItem | None],
) -> None:
    count = 0
    first_few_lines: list[str] = []
    try:
        # Step 1: Generate the `rclone` size commands
        # return f"rclone --config {RCLONE_CONFIG} --fast-list size {servers[0]}:{libname} --json"
        # rclone --config rclone.conf check 45042:libgenrs_nonfiction dst:TorrentBooks/libgenrs_nonfiction --log-level INFO --log-format json
        # return f"rclone --config {RCLONE_CONFIG} check {servers[0]}:{libname} dst:TorrentBooks/{libname} --log-level INFO --missing-on-dst missed_on_dst.txt --missing-on-src missed_on_src.txt"

        # src_str = f"{servers[0]}:{libname}"
        # dst_str = f"dst:TorrentBooks/{libname}"
        # cmd_list: list[str] = [
        #     "rclone",
        #     "--config",
        #     RCLONE_CONFIG,
        #     "check",
        #     src_str,
        #     dst_str,
        #     "--checkers",
        #     "1000",
        #     "--log-level",
        #     "INFO",
        #     "--combined",
        #     "-",
        # ]
        # cmd = subprocess.list2cmdline(cmd_list)
        # print(f"Command: {cmd}")
        # proc = subprocess.Popen(
        #     cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        # )
        assert running_process.stdout is not None
        n_max = 10
        for line in iter(running_process.stdout.readline, b""):
            try:
                line_str = line.decode("utf-8").strip()
                if len(first_few_lines) < n_max:
                    first_few_lines.append(line_str)
                if line_str.startswith("="):
                    output.put(QueueItem(LineType.EQUAL, line_str[1:].strip()))
                    count += 1
                    continue
                if line_str.startswith("-"):
                    slug = line_str[1:].strip()
                    # print(f"Missing on src: {slug}")
                    output.put(QueueItem(LineType.MISSING_ON_SRC, f"{dst_slug}/{slug}"))
                    count += 1
                    continue
                if line_str.startswith("+"):
                    slug = line_str[1:].strip()
                    output.put(QueueItem(LineType.MISSING_ON_DST, f"{src_slug}/{slug}"))
                    count += 1
                    continue
                # print(f"unhandled: {line_str}")
            except UnicodeDecodeError:
                print("UnicodeDecodeError")
                continue
        output.put(None)
        print("done")
    except KeyboardInterrupt:
        import _thread

        print("KeyboardInterrupt")
        output.put(None)
        _thread.interrupt_main()
    if count == 0:
        first_lines_str = "\n".join(first_few_lines)
        raise ValueError(
            f"No output from rclone check, first few lines: {first_lines_str}"
        )


# def batch_delete(fileset: list[str]) -> None:
#     from pathlib import Path
#     print("### BATCH DELETE ###")

#     if len(fileset) == 0:
#         print("No files to delete")
#         return

#     remote = "dst:TorrentBooks"


#     include_files_txt = Path("include_files.txt")
#     with include_files_txt.open("w") as f:
#         for file in fileset:
#             assert file.startswith(remote)
#             needle = f"{remote}/"
#             file = file[file.index(needle) + len(needle) :]
#             f.write(file + "\n")

#     print(f"Files to delete: {len(fileset)}")
#     # print("BATCH DELETE!")
#     # for file in fileset:
#     #     print(f"TODO: rclone delete {file}")
#     #     time.sleep(1)
#     cmd_list: list[str] = [
#         "rclone",
#         "--config",
#         RCLONE_CONFIG,
#         "delete",
#         remote,
#         "--files-from",
#         str(include_files_txt),
#         "--checkers",
#         "1000",
#         "--transfers",
#         "1000",
#         "-v",
#     ]
#     print(f"Command: {subprocess.list2cmdline(cmd_list)}")
#     # proc = subprocess.Popen(cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
#     rtn = subprocess.call(cmd_list)
#     print(f"Return code: {rtn}")
#     print("### BATCH DELETE ###")


# def runner_run_delete(filestream: Queue[str | None]) -> None:
#     try:
#         current_set: list[str] = []
#         item: str | None = None
#         # Batch up all files to delete then call rclone delete on the file batch.
#         while True:
#             while True:
#                 try:
#                     item = filestream.get_nowait()
#                     if item is None:
#                         break
#                     current_set.append(item)
#                 # except Queue.Empty:  # fix this
#                 except Empty:
#                     break
#             if len(current_set) == 0:
#                 time.sleep(1)
#                 continue
#             previous_set, current_set = current_set, []
#             batch_delete(previous_set)
#     except KeyboardInterrupt:
#         print("KeyboardInterrupt")
#         filestream.put(None)
#         import _thread

#         _thread.interrupt_main()


# def clean(libname: str) -> None:
#     thread_consumer: Thread | None = None
#     thread_producer: Thread | None = None
#     que_all_events: Queue[QueueItem | None] = Queue()
#     que_need_delete_on_dst: Queue[str | None] = Queue()
#     try:
#         thread_producer = Thread(
#             target=producer,
#             args=(
#                 libname,
#                 que_all_events,
#             ),
#             daemon=True,
#         )
#         thread_consumer = Thread(
#             target=runner_run_delete, args=(que_need_delete_on_dst,), daemon=True
#         )
#         thread_producer.start()
#         thread_consumer.start()
#         while item := que_all_events.get():
#             if item is None:
#                 break
#             # print(item)\
#             type, line = item.line_type, item.line
#             if type == LineType.EQUAL:
#                 continue
#             if type == LineType.MISSING_ON_SRC:
#                 que_need_delete_on_dst.put(line)
#                 #myprint(f"Missing on src: {line}")
#                 continue
#             if type == LineType.MISSING_ON_DST:
#                 #myprint(f"Missing on dst: {line}")
#                 continue
#             raise ValueError(f"Unknown type: {type}")
#     except KeyboardInterrupt:
#         print("KeyboardInterrupt")
#         que_all_events.put(None)
#         que_need_delete_on_dst.put(None)
#         if thread_producer is not None:
#             thread_producer.join(timeout=5)
#         if thread_consumer is not None:
#             thread_consumer.join(timeout=5)

# def main() -> None:
#     libs: list[str] = [
#         "meta",
#         "aa_misc_data",
#         "aa_derived_mirror_metadata",
#         "ia2_acsmpdf_files",
#         "scimag",
#         "zlib2",
#         "zlib3_files",
#     ]
#     for lib in libs:
#         print(f"\n################### Cleaning {lib} ##################")
#         clean(lib)

# if __name__ == "__main__":
#     main()
