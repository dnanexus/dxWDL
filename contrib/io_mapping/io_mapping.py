import json
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Tuple, cast

import autoclick
import dxpy
from xphyle import open_
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
    wdl: Path,
    cromwell_inputs: str = "-",
    dx_inputs: str = "-",
    task_name: Optional[str] = None,
    project_id: Optional[str] = None,
    folder: Optional[str] = None,
    upload_missing: bool = False
):
    """
    Maps Cromwell-style inputs to DNAnexus inputs.

    Args:
        wdl: Path to the WDL file.
        cromwell_inputs: Path to Cromwell-style inputs JSON file. Defaults to "-" (stdin).
        dx_inputs: The output file. Defaults to "-" (stdout).
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
    with open_(cromwell_inputs, "rt") as inp, open_(dx_inputs, "wt") as out:
        json.dump(
            InputsFormatter(
                wdl, task_name, project_id, folder, upload_missing
            ).format_inputs(json.load(inp)),
            out,
            indent=2
        )


class InputsFormatter:
    def __init__(
        self,
        wdl_path: Path,
        task_name: Optional[str] = None,
        project_id: Optional[str] = None,
        folder: Optional[str] = None,
        upload_missing: bool = False
    ):
        self._wdl_doc = parse_wdl(wdl_path)

        if not task_name:
            target = self._wdl_doc.workflow
        else:
            for task in self._wdl_doc.tasks:
                if task.name == task_name:
                    target = task
                    break
            else:
                raise ValueError(f"No such task {task_name} in WDL document")
        
        self._wdl_decls = dict((d.name, d.value.type) for d in target.available_inputs)
        self._source_prefix = f"{target.name}."
        self._dest_prefix = self._source_prefix if task_name else "stage-common."
        self._project_id = project_id or dxpy.PROJECT_CONTEXT_ID
        self._folder = folder or "/",
        self._recurse = folder is not None
        self._upload_missing = upload_missing
        self._data_file_links = set()
    
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


@dx_io_map.command(
    "outputs",
    validations={
        ("dx_id", "dx_outputs"): autoclick.Mutex(),
        ("workflow_name", "task_name"): autoclick.Mutex(),
        ("wdl", "workflow_name", "task_name"): autoclick.defined_ge(1)
    }
)
def map_outputs(
    dx_id: Optional[str] = None,
    dx_outputs: Optional[str] = None,
    cromwell_outputs: str = "-",
    wdl: Optional[Path] = None,
    workflow_name: Optional[str] = None,
    task_name: Optional[str] = None,
    strict: bool = False,
):
    """
    Maps DNAnexus job/analysis outputs to Cromwell-style outputs.

    At least one of 'wdl', 'workflow_name', and 'task_name' must be specified.

    Args:
        wdl: Path to the WDL file.
        dx_id: The ID of the DNAnexus job/analysis.
        dx_outputs: The outputs of a DNAnexus job/analysis in JSON format. Defaults to "-" if
            no `dx_id` is specified. Only one of (`dx_id`, `dx_outputs`) may be specified.
        cromwell_outputs: The output file. Defaults to "-" (stdout).
        workflow_name: The name of the WDL workflow, if this is a workflow execution (i.e. a
            DNAnexus analysis).
        task_name: The name of the WDL task, if this is a task execution (i.e. a DNAnexus job).
        strict: Only map the outputs from the workflow output stage (i.e. those prefixed with
            'stage-outputs').
    """
    is_workflow = dx_id.startswith("analysis-") if dx_id else None
    
    if not (workflow_name or task_name):
        wdl_doc = parse_wdl(wdl)
        
        if wdl_doc.workflow and is_workflow is not False:
            workflow_name = wdl_doc.workflow.name
        elif wdl_doc.tasks and len(wdl_doc.tasks) == 1 and is_workflow is not True:
            task_name = wdl_doc.tasks[0].name
        else:
            raise ValueError("The workflow or task name cannot be determined unambiguously")
    elif workflow_name and is_workflow is False:
        raise ValueError("'workflow_name' was given but 'dx_id' is not an analysis ID")
    elif task_name and is_workflow is True:
        raise ValueError("'task_name' was given but 'dx_id' is not an job ID")

    if dx_id:
        if task_name:
            exe = dxpy.DXJob(dx_id)
        else:
            exe = dxpy.DXAnalysis(dx_id)
        
        desc = exe.describe()

        if desc["state"] != "done":
            raise ValueError(
                f"DNAnexus execution {dx_id} is not in a successfully completed state"
            )
        
        outputs_dict = desc["output"]
    else:
        with open_(dx_outputs or "-", "rt") as inp:
            outputs_dict = json.load(inp)
    
    with open_(cromwell_outputs, "wt") as out:
        json.dump(
            OutputsFormatter(workflow_name, task_name, strict).format_outputs(outputs_dict),
            out,
            indent=2
        )


class OutputsFormatter:
    def __init__(
        self, 
        workflow_name: Optional[str] = None, 
        task_name: Optional[str] = None,
        strict: bool = False,
    ):
        self._source_prefix = f"{task_name}." if task_name else "stage-outputs."
        self._dest_prefix = f"{task_name or workflow_name}."
        self._strict = strict
    
    def format_outputs(self, outputs_dict: dict) -> dict:
        formatted = {}

        for key, value in outputs_dict.items():
            if key.startswith(self._source_prefix):
                key = key[len(self._source_prefix):]
            elif self._strict:
                continue

            new_key = f"{self._dest_prefix}{key}"

            formatted[new_key] = self.format_value(value)
        
        return formatted

    def format_value(self, value: Any) -> Any:
        if dxpy.is_dxlink(value):
            link = cast(dict, value)[DX_LINK_KEY]

            if isinstance(link, dict):
                link_dict = cast(dict, link)
                file_id = link_dict["id"]
                if "project" in link_dict:
                    return f"dx://{link_dict['project']}:{file_id}"
            else:
                file_id = cast(str, link)
            
            return f"dx://{file_id}"
        
        if isinstance(value, dict):
            return dict((key, self.format_value(val)) for key, val in cast(dict, value).items())

        if isinstance(value, Sequence) and not isinstance(value, str):
            return [self.format_value(val) for val in cast(Sequence, value)]

        return value
            

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
