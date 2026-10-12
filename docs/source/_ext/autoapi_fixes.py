"""Work around sphinx-autoapi limitations that the driver's sources hit.

The parser patches touch autoapi's private API and were written against
sphinx-autoapi 3.8.1; a newer version may need them adjusted.
"""

import typing
from collections.abc import Callable
from typing import Any, cast

from astroid import nodes as astroid_nodes  # pyright: ignore[reportMissingTypeStubs]
from autoapi._objects import PythonObject  # pyright: ignore[reportMissingTypeStubs]
from autoapi._parser import Parser  # pyright: ignore[reportMissingTypeStubs]
from docutils import nodes
from sphinx import addnodes
from sphinx.application import Sphinx
from sphinx.environment import BuildEnvironment
from sphinx.transforms.post_transforms import SphinxPostTransform
from sphinx.util.typing import ExtensionMetadata

# What autoapi's parser returns for one node: a list of object descriptions.
ParsedData = list[dict[str, Any]]


def _skip_nested_classes_of_bases() -> None:
    # PyO3 enum variants are nested subclasses of their enum
    # (RetryDecision.RetrySameTarget). autoapi parses a base's nested classes
    # while collecting inherited members, which recurses forever on them.
    orig_parse_class_body = cast(
        Callable[[Parser, astroid_nodes.ClassDef, bool], dict[str, Any]],
        Parser._parse_classdef,  # pyright: ignore[reportPrivateUsage]
    )
    orig_parse_class = cast(Callable[[Parser, astroid_nodes.ClassDef], ParsedData], Parser.parse_classdef)
    in_base = 0

    def parse_class_body(self: Parser, node: astroid_nodes.ClassDef, use_name_stacks: bool) -> dict[str, Any]:
        nonlocal in_base
        # use_name_stacks is False only when node is being parsed as a base.
        if use_name_stacks:
            return orig_parse_class_body(self, node, use_name_stacks)
        in_base += 1
        try:
            return orig_parse_class_body(self, node, use_name_stacks)
        finally:
            in_base -= 1

    def parse_class(self: Parser, node: astroid_nodes.ClassDef) -> ParsedData:
        if in_base:
            return []
        return orig_parse_class(self, node)

    Parser._parse_classdef = parse_class_body  # pyright: ignore[reportPrivateUsage]
    Parser.parse_classdef = parse_class


def _document_type_checking_blocks() -> None:
    # autoapi keeps only the first definition of an if block. For
    # `if TYPE_CHECKING:` document all of them, as type checkers see them.
    parse = cast(
        Callable[[Parser, astroid_nodes.NodeNG], ParsedData],
        Parser.parse,  # pyright: ignore[reportUnknownMemberType] - unannotated in autoapi.
    )

    def parse_if(self: Parser, node: astroid_nodes.If) -> ParsedData:
        if node.test.as_string() in ("TYPE_CHECKING", "typing.TYPE_CHECKING"):
            return [data for child in node.body for data in parse(self, child)]
        for child in node.get_children():
            data = parse(self, child)
            if data:
                return data
        return []

    setattr(Parser, "parse_if", parse_if)  # noqa: B010 - Parser has no parse_if to assign to.


def _skip_reexports(
    app: Sphinx, what: str, name: str, obj: PythonObject, skip: bool, options: list[str]
) -> bool | None:
    original = cast(str, cast(dict[str, Any], obj.obj).get("original_path", ""))
    if original.startswith("scylla.") and "._rust." not in original:
        # Re-exported from another public module, documented there.
        return True
    return None


def _resolve_private_reference(
    app: Sphinx, env: BuildEnvironment, node: addnodes.pending_xref, contnode: nodes.TextElement
) -> nodes.reference | None:
    # Link the scylla._rust names that remain in signatures to their public docs.
    target = node.get("reftarget", "")
    if node.get("refdomain") != "py":
        return None
    if target in ("scylla._rust.types.UnsetType", "UnsetType"):
        # UnsetType has no public name (statement.py deletes it), so link the UNSET singleton it types.
        public = "scylla.statement.UNSET"
    elif target.startswith("scylla._rust."):
        # Protocols in public modules, like RetrySession, use types that module imports from its
        # scylla._rust counterpart (RequestInfo); it is documented under the same path without _rust.
        public = target.replace("scylla._rust.", "scylla.", 1)
    else:
        return None
    node["reftarget"] = public
    # "obj", as a type alias is data, not a class.
    return env.domains.python_domain.resolve_xref(env, node["refdoc"], app.builder, "obj", public, node, contnode)


class _QualifyTypingNames(SphinxPostTransform):
    # Signatures name typing imports bare, and Sphinx's fuzzy lookup would
    # link `Any` to Consistency.Any. Point them at the typing docs instead.
    default_priority = 5  # Before ReferencesResolver (10).

    def run(self, **kwargs: Any) -> None:
        for node in self.document.findall(addnodes.pending_xref):
            target = node.get("reftarget", "")
            if node.get("refdomain") == "py" and node.get("reftype") == "class" and target in typing.__all__:
                node["reftarget"] = f"typing.{target}"
                # "obj", as the Python inventory lists most of typing as data.
                node["reftype"] = "obj"


def setup(app: Sphinx) -> ExtensionMetadata:
    _skip_nested_classes_of_bases()
    _document_type_checking_blocks()
    app.add_post_transform(_QualifyTypingNames)
    app.connect("autoapi-skip-member", _skip_reexports)
    app.connect("missing-reference", _resolve_private_reference)
    return {"parallel_read_safe": True}
