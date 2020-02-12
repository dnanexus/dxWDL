import json
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Tuple, cast

import autoclick
import dxpy
from WDL import Document, Tree, Type


try:
    # test whether the user is logged in
    assert dxpy.SECURITY_CONTEXT
    assert dxpy.whoami()
    LOGGED_IN = True
except:
    LOGGED_IN = False

DX_LINK_KEY = "$dnanexus_link"
DX_DICT_KEY = "___"
DX_FILES_SUFFIX = "___dxfiles"


@autoclick.group(main=True)
def dx_io_map():
    pass


@dx_io_map.command("inputs")
def map_inputs(
    cromwell_inputs: Path,
    wdl: Path,
    dxwdl_inputs: Optional[Path] = None,
    task_name: Optional[str] = None,
    project_id: Optional[str] = None,
    folder: Optional[str] = None,
    upload_missing: bool = False
):
    """
    Maps Cromwell-style inputs to DNAnexus inputs.

    Args:
        cromwell_inputs: Path to Cromwell-style inputs JSON file.
        wdl: Path to the WDL file.
        dxwdl_inputs: The output file; if not specified, the `cromwell_inputs` path is used with
            a ".dx.json" extension.
        task_name: The task name; if not specified, it is assumed that the inputs are for the
            workflow.
        project_id: The ID of the DNAnexus project to search for any files that are provided as
            local paths, and where any missing local files are uploaded if `upload_missing` is
            `True`. Defaults to the currently selected project.
        folder: The folder of `project_id` to search for any files that are provided as
            local paths, and where any missing local files are uploaded if `upload_missing` is
            `True`. Defaults to "/".
        upload_missing: Whether to upload any files that are provided as local paths and are not
            found in the specified project and folder.
    """
    wdl_doc = parse_wdl(wdl)
    
    with open(cromwell_inputs, "rt") as inp:
        inputs_dict = json.load(inp)
    
    formatter = DxInputsFormatter(wdl_doc, task_name, project_id, folder, upload_missing)
    dx_inputs_dict = formatter.format_inputs(inputs_dict)

    if dxwdl_inputs is None:
        dxwdl_inputs = cromwell_inputs.with_suffix(".dx.json")
    
    with open(dxwdl_inputs, "wt") as out:
        json.dump(dx_inputs_dict, out)


@dx_io_map.command(
    "outputs",
    validations={
        ("analysis_id", "analysis_outputs"): autoclick.Mutex()
    }
)
def map_outputs(
    wdl: Path,
    analysis_id: Optional[str] = None,
    analysis_outputs: Optional[Path] = None,
    cromwell_outputs: Optional[Path] = None,
):
    pass


def parse_wdl(
    wdl_path: Path,
    import_dirs: Optional[Sequence[Path]] = (),
    check_quant: bool = False,
) -> Document:
    return Tree.load(
        str(wdl_path),
        path=[str(path) for path in import_dirs],
        check_quant=check_quant
    )


class DxInputsFormatter:
    def __init__(
        self,
        wdl_doc: Document,
        task_name: Optional[str] = None,
        project_id: Optional[str] = None,
        folder: Optional[str] = None,
        upload_missing: bool = False
    ):
        if not task_name:
            target = wdl_doc.workflow
        else:
            for task in wdl_doc.tasks:
                if task.name == task_name:
                    target = task
                    break
            else:
                raise ValueError(f"No such task {task_name} in WDL document")
        
        self._data_file_links = set()
        self._wdl_doc = wdl_doc
        self._wdl_decls = dict((d.name, d.value.type) for d in target.available_inputs)
        self._source_prefix = f"{target.name}."
        self._dest_prefix = self._source_prefix if task_name else "stage-common."
        self._project_id = project_id or dxpy.PROJECT_CONTEXT_ID
        self._folder = folder or "/",
        self._recurse = folder is not None
        self._upload_missing = upload_missing
    
    def format_inputs(self, inputs_dict: dict) -> dict:
        formatted = {}

        for key, value in inputs_dict.items():
            if key.startswith(self._source_prefix):
                key = key[len(self._source_prefix):]
            
            new_key = f"{self._dest_prefix}{key}"

            formatted[new_key] = self.format_value(value, (key,))

            if self._data_file_links:
                formatted[f"{new_key}{DX_FILES_SUFFIX}"] = list(self._data_file_links)
                self._data_file_links.clear()

        return formatted

    def format_value(self, value: Any, path: Tuple[str, ...]) -> Any:
        """
        Convert a primitive, DataFile, Sequence, or Dict to a JSON-serializable object.
        
        Args:
            value: The value to format.
            path: The path to the current value.
        
        Returns:
            The serializable value.
        """
        if self._is_wdl_type(path, Type.File):
            if isinstance(value, dict):
                # it's already a dx link
                return cast(dict, value)
            else:
                # it's a URL or local path
                return self._format_file(cast(str, value))

        if isinstance(value, dict):
            return self._format_dict(cast(dict, value), path)

        if isinstance(value, Sequence) and not isinstance(value, str):
            return self._format_sequence(cast(Sequence, value), path)

        return value

    def _format_sequence(self, s: Sequence, path: Tuple[str, ...]) -> list:
        return [self.format_value(val, path) for val in s]

    def _format_dict(self, d: dict, path: Tuple[str, ...]) -> dict:
        if self._is_wdl_type(path, Type.Map):
            formatted_dict = {"keys": [], "values": []}

            for key, val in d.items():
                formatted_dict["keys"].append(key)
                formatted_dict["values"].append(self.format_value(val, path + (key,)))
        else:
            # struct
            formatted_dict = dict(
                (key, self.format_value(val, path + (key,))) for key, val in d.items()
            )

        return {DX_DICT_KEY: formatted_dict}

    def _is_wdl_type(self, path: Tuple[str, ...], wdl_type: Type) -> bool:
        wdl_decls = self._wdl_decls
        path_len = len(path)

        for i, key in enumerate(path, 1):
            is_last = i == path_len

            for name, type_ in wdl_decls.items():
                if name in {None, key}:
                    if is_last:
                        return isinstance(type_, wdl_type)

                    if isinstance(type_, Type.Array):
                        type_ = cast(Type.Array, type_).item_type

                    if isinstance(type_, Type.Map):
                        type_ = cast(Type.Map, type_).item_type[1]
                        # None matches any key
                        wdl_decls: Dict = {None: type_}
                    elif isinstance(type_, Type.StructInstance):
                        wdl_decls = \
                            self._wdl_doc.struct_typedefs[type_.type_name].members

                    break
            else:
                raise ValueError(
                    f"No input matching key {key} at {path[:i]}"
                )

    def _format_file(self, url_or_path: str) -> dict:
        if url_or_path.startswith("dx://"):
            return dxpy.dxlink(*url_or_path[5:].split(":"))

        if not LOGGED_IN:
            raise ValueError(
                f"File {url_or_path} is not a dx:// URL, and you are not currently logged into "
                f"DNAnexus, so the file cannot be searched or uploaded."
            )
        
        local_path = Path(url_or_path)

        existing_files = list(dxpy.find_data_objects(
            classname="file",
            state="closed",
            name=local_path.name,
            project=self._project_id,
            folder=self._folder,
            recurse=self._recurse,
        ))

        if len(existing_files) == 1:
            return dxpy.dxlink(existing_files[0]["id"], self._project_id)
        elif len(existing_files) > 1:
            raise ValueError(
                f"Multiple files with name {local_path.name} found in "
                f"{self._project_id}:{self._folder}"
            )
        elif not self._upload_missing:
            raise ValueError(
                f"Local file {local_path} not found in {self._project_id}:{self._folder} "
                f"and 'upload_missing' is set to 'False'"
            )
        elif not local_path.exists():
            raise ValueError(f"File {local_path} does not exist")
        else:
            return dxpy.dxlink(dxpy.upload_local_file(
                str(local_path),
                name=local_path.name,
                project=self._project_id,
                folder=self._folder,
                parents=True,
                wait_on_close=True
            ))
