from dataclasses import dataclass


@dataclass
class FilePathParts:
    """File path dataclass."""

    parents: list[str]
    name: str


def parse_file(file_path: str) -> FilePathParts:
    """Parse file path into parts."""
    assert not file_path.endswith("/"), "This looks like a directory path"
    if file_path.startswith("/"):
        file_path = file_path[1:]
    parents = file_path.split("/")
    if len(parents) == 1:
        return FilePathParts(parents=[], name=parents[0])
    name = parents.pop()
    return FilePathParts(parents=parents, name=name)


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
        while node := node.parent:
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


def _make_tree(files: list[str], fully_qualified: bool) -> dict[str, TreeNode]:
    tree: dict[str, TreeNode] = {}
    for file in files:

        parts = parse_file(file)
        if not fully_qualified:
            remote = "root"
        else:
            remote = file.split(":", 1)[0]

        
        node: TreeNode = tree.setdefault(remote, TreeNode(remote))
        if not parts.parents:
            node.files.append(parts.name)
            node.add_count_bubble_up()
            continue
        for parent in parts.parents:
            is_last = parent == parts.parents[-1]
            node = node.child_nodes.setdefault(parent, TreeNode(parent, parent=node))
            if is_last:
                node.files.append(parts.name)
                node.add_count_bubble_up()
    return tree


#
def _fixup_rclone_paths(
    outpaths: dict[str, list[str]], fully_qualified: bool
) -> dict[str, list[str]]:
    prefix = "/root" if not fully_qualified else ""
    out: dict[str, list[str]] = {}
    for path, files in outpaths.items():
        # fixup path
        assert path.startswith(prefix), f"Path should start with {prefix}"
        path = path[len(prefix) :]
        if path.startswith("/"):
            path = path[1:]
        out[path] = files
    return out


def group_files(files: list[str], fully_qualified=True) -> dict[str, list[str]]:
    """split between filename and parent directory path"""
    tree: dict[str, TreeNode] = _make_tree(files, fully_qualified=fully_qualified)
    outpaths: dict[str, list[str]] = {}
    for _, node in tree.items():
        _merge(node, "", outpaths)
    out: dict[str, list[str]] = _fixup_rclone_paths(
        outpaths=outpaths, fully_qualified=fully_qualified
    )
    return out


__all__ = ["group_files"]
