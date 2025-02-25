from dataclasses import dataclass
from pathlib import Path


@dataclass
class PrefixResult:
    prefix: str
    files: list[str]


@dataclass
class FilePathParts:
    """File path dataclass."""

    remote: str
    parents: list[str]
    name: str

    def to_string(self, include_remote: bool, include_bucket: bool) -> str:
        """Convert to string, may throw for not include_bucket=False."""
        parents = list(self.parents)
        if not include_bucket:
            parents.pop(0)
        path = "/".join(parents)
        if path:
            path += "/"
        path += self.name
        if include_remote:
            return f"{self.remote}{path}"
        return path


def parse_file(file_path: str) -> FilePathParts:
    """Parse file path into parts."""
    assert not file_path.endswith("/"), "This looks like a directory path"
    parts = file_path.split(":")
    remote = parts[0]
    path = parts[1]
    if path.startswith("/"):
        path = path[1:]
    parents = path.split("/")
    if len(parents) == 1:
        return FilePathParts(remote=remote, parents=[], name=parents[0])
    name = parents.pop()
    return FilePathParts(remote=remote, parents=parents, name=name)


class TreeNode:
    def __init__(
        self,
        name: str,
        child_nodes: dict[str, "TreeNode"] | None = None,
        files: list[str] | None = None,
        parent: "TreeNode | None" = None,
    ):
        self.name = name
        self.child_nodes = child_nodes or {}
        self.files = files or []
        self.count = 0
        self.parent = parent

    def add_count_bubble_up(self):
        self.count += 1
        if self.parent:
            self.parent.add_count_bubble_up()

    def get_path(self) -> str:
        paths_reversed: list[str] = [self.name]
        node: TreeNode | None = self
        assert node is not None
        while True:
            node = node.parent
            if node is None:
                break
            paths_reversed.append(node.name)
        return "/".join(reversed(paths_reversed))

    def get_child_subpaths(self, parent_path: str | None = None) -> list[str]:
        paths: list[str] = []
        for child in self.child_nodes.values():
            child_paths = child.get_child_subpaths(parent_path=child.name)
            paths.extend(child_paths)
        for file in self.files:
            if parent_path:
                file = f"{parent_path}/{file}"
            paths.append(file)
        return paths

    def __repr__(self, indent: int = 0) -> str:
        # return f"{self.name}: {self.count}, {len(self.children)}"
        leftpad = " " * indent
        msg = f"{leftpad}{self.name}: {self.count}"
        if self.child_nodes:
            # msg += f"\n   {len(self.children)} children"
            msg += "\n"
            for child in self.child_nodes.values():
                if isinstance(child, TreeNode):
                    msg += child.__repr__(indent + 2)
                else:
                    msg += f"{leftpad}  {child}\n"
        return msg


def _merge(node: TreeNode, parent_path: str, out: dict[str, list[str]]) -> None:
    parent_path = parent_path + "/" + node.name
    if not node.child_nodes and not node.files:
        return  # done
    if node.files:
        # we saw files, to don't try to go any deeper.
        filelist = out.setdefault(parent_path, [])
        paths = node.get_child_subpaths()
        for path in paths:
            filelist.append(path)
        out[parent_path] = filelist
        return

    n_child_nodes = len(node.child_nodes)

    if n_child_nodes <= 2:
        for child in node.child_nodes.values():
            _merge(child, parent_path, out)
        return

    filelist = out.setdefault(parent_path, [])
    paths = node.get_child_subpaths()
    for path in paths:
        filelist.append(path)
    out[parent_path] = filelist
    return


def _make_tree(files: list[str]) -> dict[str, TreeNode]:
    tree: dict[str, TreeNode] = {}
    for file in files:
        parts = parse_file(file)
        remote = parts.remote
        node: TreeNode = tree.setdefault(remote, TreeNode(remote))
        if parts.parents:
            for parent in parts.parents:
                is_last = parent == parts.parents[-1]
                node = node.child_nodes.setdefault(
                    parent, TreeNode(parent, parent=node)
                )
                if is_last:
                    node.files.append(parts.name)
                    node.add_count_bubble_up()
        else:
            node.files.append(parts.name)
            node.add_count_bubble_up()

    return tree


#
def _fixup_rclone_paths(outpaths: dict[str, list[str]]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for path, files in outpaths.items():
        # fixup path
        assert path.startswith("/"), "Path should start with /"
        path = path[1:]
        # replace the first / with :
        path = path.replace("/", ":", 1)
        out[path] = files
    return out


def group_files(files: list[str], fully_qualified: bool = True) -> dict[str, list[str]]:
    """split between filename and parent directory path"""
    if fully_qualified is False:
        for i, file in enumerate(files):
            file = "root:" + file
            files[i] = file
    tree: dict[str, TreeNode] = _make_tree(files)
    outpaths: dict[str, list[str]] = {}
    for _, node in tree.items():
        _merge(node, "", outpaths)
    tmp: dict[str, list[str]] = _fixup_rclone_paths(outpaths=outpaths)
    out: dict[str, list[str]] = {}
    if fully_qualified is False:
        for path, files in tmp.items():
            if path.startswith("root"):
                path = path.replace("root", "")
                if path.startswith(":"):
                    path = path[1:]
            out[path] = [file.replace("/root/", "") for file in files]
    else:
        out = tmp
    return out


def group_under_remote_bucket(
    files: list[str], fully_qualified: bool = True
) -> dict[str, list[str]]:
    """split between filename and bucket"""
    assert fully_qualified is True, "Not implemented for fully_qualified=False"
    out: dict[str, list[str]] = {}
    for file in files:
        parsed = parse_file(file)
        remote = f"{parsed.remote}:"
        parts = parsed.parents
        bucket = parts[0]
        remote_bucket = f"{remote}{bucket}"
        file_list = out.setdefault(remote_bucket, [])
        file_list.append(parsed.to_string(include_remote=False, include_bucket=False))
    return out


def _get_prefix(path: str) -> tuple[str, str] | None:
    path_path: Path = Path(path)
    parts = path_path.parts
    if len(parts) == 1:
        return None
    return parts[0], "/".join(parts[1:])


def _common_prefix(prefix: str, files: list[str]) -> PrefixResult:
    if not files:
        return PrefixResult(prefix=prefix, files=[])
    prefix = prefix
    tmp: list[str] = list(files)
    while True:
        if not tmp:
            break
        prefix_set: set[str | None] = set()
        for file in tmp:
            pair = _get_prefix(file)
            if pair is None:
                break
            _prefix, _ = pair
            prefix_set.add(_prefix)
        if len(prefix_set) > 1 or len(prefix_set) == 0:
            break
        next_prefix: str | None = prefix_set.pop()
        if next_prefix is None:
            break
        prefix += f"/{next_prefix}"
        new_tmp: list[str] = []
        for file in tmp:
            pair = _get_prefix(file)
            assert pair is not None
            _, path = pair
            new_tmp.append(path)
        tmp = new_tmp
    return PrefixResult(prefix=prefix, files=tmp)


def group_under_one_prefix(prefix: str, files: list[str]) -> tuple[str, list[str]]:
    """Group files under one prefix."""
    if not files:
        return prefix, []
    prefix = prefix
    result = _common_prefix(prefix, files)
    return result.prefix.replace(":/", ":"), result.files


__all__ = ["group_files", "group_under_remote_bucket", "group_under_one_prefix"]
