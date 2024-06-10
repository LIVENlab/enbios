import io
import json
from logging import getLogger, Logger, StreamHandler
from pathlib import Path
from typing import Optional, TextIO, cast, Union

import docspec
import pystache
from pydantic import BaseModel, Field
from pydoc_markdown import MarkdownRenderer, Context, Renderer, PythonLoader
from pydoc_markdown.contrib.processors.sphinx import SphinxProcessor
from pydoc_markdown.util.misc import escape_except_blockquotes
from pystache.parsed import ParsedTemplate

from enbios import Experiment
from enbios.const import PROJECT_PATH, BASE_TEST_DATA_PATH


class FunctionRenderConfig(BaseModel):
    sphinx_process: bool = True
    render_header: bool = True
    render_signature: bool = True
    render_docstring: bool = True


class ClassRenderConfig(BaseModel):
    render_header: bool = True
    render_signature: bool = False
    render_docstring: bool = True
    render_init: bool = False
    render_public_functions: bool = True
    render_private_functions: bool = False
    function_config: Optional[FunctionRenderConfig] = Field(
        default_factory=FunctionRenderConfig
    )


class ModuleRenderConfig(BaseModel):
    render_header: bool = True
    render_docstring: bool = True
    render_public_functions: bool = True
    render_private_functions: bool = False


class RenderConfig(BaseModel):
    module_config: Optional[ModuleRenderConfig] = Field(
        default_factory=ModuleRenderConfig
    )
    class_config: Optional[ClassRenderConfig] = Field(default_factory=ClassRenderConfig)
    function_config: Optional[FunctionRenderConfig] = Field(
        default_factory=FunctionRenderConfig
    )


class InsertAPIObjectParsedTemplate(ParsedTemplate):
    def __init__(
        self,
        pt: ParsedTemplate,
        context: Context,
        py_loader: PythonLoader,
        renderer: Optional[Renderer] = None,
        config: Optional[RenderConfig] = None,
        logger: Optional[Logger] = None,
    ):
        super(InsertAPIObjectParsedTemplate, self).__init__()
        self._parse_tree = pt._parse_tree
        self.context = context
        self._py_loader = py_loader
        self._py_loader.init(context)

        self._module_list = list(self._py_loader.load())

        if config:
            self._config = config
        else:
            self._config = RenderConfig()

        self.sphinx_proc = SphinxProcessor()

        if renderer:
            self._renderer = renderer
        else:
            self._renderer = MarkdownRenderer(
                render_module_header=False,
                # render_signature=True,  # Ensure function signatures are rendered
                add_member_class_prefix=False,  # Avoids adding class prefixes to members
                # code_block_style='fenced',  # Use fenced code blocks (```)
                insert_header_anchors=False,  # Avoid inserting anchors in headers
                render_toc=False,
            )  # Disable rendering of table of contents
        if logger:
            self.logger = logger
        else:
            self.logger = getLogger("InsertAPIObjectParsedTemplate")
            self.logger.addHandler(StreamHandler())
            self.logger.propagate = False

        self._renderer.init(context)

    def find_object(
        self, obj_path: str
    ) -> Union[docspec.ApiObject, docspec.Function, docspec.Class, tuple[Path, str]]:
        path_parts = obj_path.split(".")
        if obj_path.startswith("file:"):
            file_type_and_path = obj_path.removeprefix("file:")
            type_prefix_end_index = file_type_and_path.index(":")
            type_prefix = file_type_and_path[:type_prefix_end_index]
            file_path = PROJECT_PATH / file_type_and_path.removeprefix(f"{type_prefix}:")
            if file_path.exists():
                return file_path, type_prefix
            else:
                raise FileNotFoundError(f"File {file_path} not found.")
        module = list(
            filter(
                lambda m: m.name == path_parts[0] or obj_path.startswith(m.name),
                self._module_list,
            )
        )
        if not module:
            raise ModuleNotFoundError(
                f"Module {path_parts[0]} not found. Options: {[m.name for m in self._module_list]}"
            )
        else:
            current_obj: docspec.ApiObject = list(
                sorted(module, key=lambda m: len(m.name))
            )[-1]

        path_parts = path_parts[len(current_obj.name.split(".")) :]
        for part in path_parts:
            next_: docspec.ApiObject = docspec.get_member(current_obj, part)
            if not next_:
                raise ModuleNotFoundError(
                    f"Member '{part}' not found in {obj_path}. Valid subpath: {current_obj.path}"
                )
            current_obj = next_
        return current_obj

    def _render_docstring(self, obj_: docspec.ApiObject, fp: Optional[TextIO] = None):
        docstring = (
            escape_except_blockquotes(obj_.docstring.content)
            if self._renderer.escape_html_in_docstring
            else obj_.docstring.content
        )
        lines = docstring.split("\n")
        if self._renderer.docstrings_as_blockquote:
            lines = ["> " + x for x in lines]
        fp.write("\n".join(lines))
        fp.write("\n\n")

    def _render_module(
        self,
        obj_: docspec.Module,
        fp: Optional[TextIO] = None,
        _config: Optional[ModuleRenderConfig] = None,
    ):
        if not _config:
            _config = self._config.module_config

        if _config.render_header:
            self._renderer._render_header(fp, 3, obj_)

        # if _config.render_signature:
        #     self._renderer._render_signature_block(fp, obj_)

        if _config.render_docstring and obj_.docstring:
            self._render_docstring(obj_, fp)

        if self._config.class_config.render_init:
            constructor: docspec.Function = cast(
                Optional[docspec.Function], docspec.get_member(obj_, "__init__")
            )
            if constructor:
                self._render_function(
                    constructor, fp, self._config.class_config.function_config
                )

        for member in obj_.members:
            if isinstance(member, docspec.Function):
                if member.name == "__init__":
                    continue
                if member.name.startswith("_"):
                    if _config.render_private_functions:
                        self._render_function(
                            member, fp, self._config.class_config.function_config
                        )
                elif _config.render_public_functions:
                    self._render_function(
                        member, fp, self._config.class_config.function_config
                    )

    def _render_class(
        self,
        obj_: docspec.Class,
        fp: Optional[TextIO] = None,
        _config: Optional[ClassRenderConfig] = None,
    ):
        if not _config:
            _config = self._config.class_config

        if _config.render_header:
            self._renderer._render_header(fp, 3, obj_)

        if _config.render_signature:
            self._renderer._render_signature_block(fp, obj_)

        if _config.render_docstring and obj_.docstring:
            self._render_docstring(obj_, fp)

        if self._config.class_config.render_init:
            constructor: docspec.Function = cast(
                Optional[docspec.Function], docspec.get_member(obj_, "__init__")
            )
            if constructor:
                self._render_function(
                    constructor, fp, self._config.class_config.function_config
                )

        for member in obj_.members:
            if isinstance(member, docspec.Function):
                if member.name == "__init__":
                    continue
                if member.name.startswith("_"):
                    if _config.render_private_functions:
                        self._render_function(
                            member, fp, self._config.class_config.function_config
                        )
                elif _config.render_public_functions:
                    self._render_function(
                        member, fp, self._config.class_config.function_config
                    )

    def _render_function(
        self,
        obj_: docspec.Function,
        fp: TextIO,
        _config: Optional[FunctionRenderConfig] = None,
    ):
        if not _config:
            _config = self._config.function_config

        if _config.sphinx_process:
            docspec.visit([obj_], self.sphinx_proc._process)

        if _config.render_header:
            self._renderer._render_header(fp, 3, obj_)

        if _config.render_signature:
            self._renderer._render_signature_block(fp, obj_)

        if _config.render_docstring and obj_.docstring:
            self._render_docstring(obj_, fp)

    def render(self, fp: TextIO):
        """
        Returns: a string of type unicode.

        Also include file:<filetype>:<filepath> to include the contents of a file. as code-block of type <filetype>
        """

        def get_unicode(node):
            if isinstance(node, str):
                return node
            try:
                obj_ = self.find_object(node.key)
                fp = io.StringIO()
                if isinstance(obj_, tuple) and isinstance(obj_[0], Path):
                    file_path, file_type = obj_
                    with open(file_path, "r") as f:
                        fp.write(f"```{file_type}\n{f.read()}\n```")
                # elif isinstance(obj_, docspec.ApiObject):
                #     self._renderer._render_header(fp, 3, obj_)
                elif isinstance(obj_, docspec.Function):
                    self._render_function(obj_, fp)
                elif isinstance(obj_, docspec.Class):
                    self._render_class(obj_, fp)
                elif isinstance(obj_, docspec.Module):
                    self._render_module(obj_, fp)
                else:
                    raise NotImplementedError(f"Unsupported object type: {type(obj_)}")
                return fp.getvalue()
            except ModuleNotFoundError as err:
                self.logger.warning(err)
                return ""

        result = str("".join((map(get_unicode, self._parse_tree))))

        fp.write(result)


def create_fundamentals():
    # pre-create some json files
    # bw adapter config:
    adapter_configs = Experiment.get_builtin_adapters()
    base_gen_path = BASE_TEST_DATA_PATH / "docs_data/gen/"
    json.dump(
        adapter_configs["brightway-adapter"],
        (base_gen_path / "bw_adapter.json").open("w", encoding="utf-8"),
        ensure_ascii=False,
        indent=2,
    )

    template_path = PROJECT_PATH / "docs_templates/Fundamentals.md"
    dest_path = PROJECT_PATH / "docs/Fundamentals.md"
    with open(template_path, encoding="utf-8") as fin:
        text = fin.read()
        pystache_parsed = pystache.parse(text)
        template_module_parsed: InsertAPIObjectParsedTemplate = (
            InsertAPIObjectParsedTemplate(
                pystache_parsed,
                context=Context(directory=PROJECT_PATH.as_posix()),
                py_loader=PythonLoader(search_path=["."]),
                config=RenderConfig(class_config=ClassRenderConfig()),
            )
        )
        template_module_parsed.render(dest_path.open("w"))
        print(
            f"Created {dest_path.relative_to(PROJECT_PATH)} from {template_path.relative_to(PROJECT_PATH)}"
        )


if __name__ == "__main__":
    create_fundamentals()
