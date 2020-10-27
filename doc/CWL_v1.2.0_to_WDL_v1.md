# CWL v1.2.0 to WDL v1.0 mapping

* Author: Michael R. Crusoe on behalf of DNAnexus, Inc.
* Copyright: 2020 DNAnexus, Inc.
* Date: 2020-07-06

## Introduction

This document maps the concepts from the CWL standards v1.2.0 to the OpenWDL v1.0 specification. Implementation hints with regards to the Cromwell and dxWDL engines and their conventions are also provided. If the implementation could be simplified by using part of the CWL reference implementation ("cwltool") for command line rendering and Docker invocation, then this is indicated along with non-cwltool-based suggestions.

## Sources

* [CWL v1.2.0 CommandLineTool specification](https://www.commonwl.org/v1.2.0/CommandLineTool.html)
* [CWL v1.2.0 Workflow specification](https://www.commonwl.org/v1.2.0/Workflow.html)
* [CWL v1.2.0 Schema Salad specfication](https://www.commonwl.org/v1.2/SchemaSalad.html)
* [WDL 1.0 specification](https://github.com/openwdl/wdl/blob/9049a884b56aebb84bce5b9a164e84584cc573ac/versions/1.0/SPEC.md) (2020-06-23)
* [WDL 2.0 (development) specification](https://github.com/openwdl/wdl/blob/main/versions/development/SPEC.md)
* [Cromwell-supported runtime attributes](https://cromwell.readthedocs.io/en/stable/RuntimeAttributes/)
* [dxWDL extensions](https://github.com/dnanexus/dxWDL/blob/41f7ee24961fdd782a519f3459d06c7780376f3b/doc/ExpertOptions.md#extensions) (2020-06-23)

## CWL to WDL type mappings

| CWL type    | WDL type  | Notes |
|-------------|-----------|-------|
| `null`      | NA*       | In type definitions can be mapped to "`?`" or “`+`” (optionality) in WDL. In user objects it maps to WDL’s `null`. * In WDL 2.0 there is a `None` type that directly maps to CWL `null`. |
| `boolean`   | `Boolean` | |
| `int`       | `Int`     | In wdlTools, Int is implemented as Scala Long |
| `long`      | `Int`     | |
| `float`     | `Float`   | In wdlTools, Float is implemented as Scala Double |
| `double`    | `Float`   | |
| `string`    | `String`  | |
| `File`      | `File`    | There is no direct analogue in WDL for most of the CWL `File` properties: `dirname`, `nameroot`, `nameext`, `checksum`, `secondaryFiles`, `format`, or `contents`. * CWL `File.basename` property -> WDL `basename(File)` function. * CWL’s `File.size` property -> WDL `size(File)` function. |
| `Directory` | NA*       | * The `Directory` type is added in WDL 2.0 |
| [`CommandInputArraySchema`](https://www.commonwl.org/v1.2/CommandLineTool.html#CommandInputArraySchema) | WDL `Array` | CWL arrays can have multiple item types, but WDL arrays cannot. * The CWL parameter reference `$(array.length)` is equivalent to WDL the `length(array)` function call. * The `inputBinding` for a CWL array can be rewritten as part of the command line rendering, but it must not be lost. |
| [`CommandInputEnumSchema`](https://www.commonwl.org/v1.2/CommandLineTool.html#CommandInputEnumSchema) | NA | Could be reduced to a WDL `String` by throwing away the value constraint and GUI hint. Could also be represented using a WDL `Struct`. * For dxWDL the choice constraint can be represented by [`parameter_meta.choices`](https://github.com/dnanexus/dxWDL/blob/master/doc/ExpertOptions.md#parameter_meta-section). |
| [`CommandInputRecordSchema`](https://www.commonwl.org/v1.2/CommandLineTool.html#CommandInputRecordSchema) | WDL Struct | |
| `?` type postfix | `?` type postfix | |

## CWL Record to WDL Struct

https://www.commonwl.org/v1.2/CommandLineTool.html#CommandInputRecordSchema & https://www.commonwl.org/v1.2/CommandLineTool.html#CommandOutputRecordSchema

CWL Record type declaration

| CWL Record field name | WDL representation | Notes |
|-----------------------|--------------------|-------|
| `type`                | NA                 | Always `type: record` |
| `fields`              | struct declarations | |
| `label`               | Could go into `parameter_meta {}` | For [dxWDL](https://github.com/dnanexus/dxWDL/blob/master/doc/ExpertOptions.md#meta-section), would map to `parameter_meta.id.label`. | Note that a record type definition might be used for multiple inputs and/or outputs |
| `doc`                 | Could go into `parameter_meta {}` | |
| `name`                | the WDL `struct` name | Not all CWL records have a name, so generate a reproducible hash based name. The CWL type system does not have a global namespace, so you will need to rename CWL Records as they are converted to WDL; you could have multiple CWL records with the same name (but different structure) in the same CWL workflow DAG (as inputs or outputs of different steps).  |
| `inputBinding`        | transformed into entries in the `command {}` block | The inputBinding for a CWL record can be rewritten as part of the command line rendering, but it must not be lost. Note that the `self` is the entire record object. |
| `outputBinding`       | varies | Uncommon. see the [`outputBinding`](#outputbinding) section |

CWL Record field declaration (from `fields` in the CWL Record type declaration)

https://www.commonwl.org/v1.2/CommandLineTool.html#CommandInputRecordField & https://www.commonwl.org/v1.2/CommandLineTool.html#CommandOutputRecordField

| CWL Record Field field name | WDL representation | Notes |
|-----------------------------|--------------------|-------|
| `name`                      | the name of the WDL struct declaration | |
| `type`                      | varies | see the [table](#cwl-to-wdl-type-mappings) |
| `doc` and `label`           | none available     | WDL appears to lack a place to document the members of a WDL struct |
| `secondaryFiles`, `streamable`, `format`, `loadContents`, `loadListing` | varies | See the guidance in the [inputs](#inputs) section |
| `inputBinding`              | transformed into entries in the `command {}` block | The inputBinding for a CWL record can be rewritten as part of the command line rendering, but it must not be lost. Note that the `self` is this particular field of the CWL record object. |
| `outputBinding`             | varies | Uncommon. See the [`outputBinding`](#outputbinding) section |

## Document metadata

CWL documents allow unlimited metadata using user-referenced vocabularies. For generic metadata the schema.org ontology is recommended by the [CWL standards](https://www.commonwl.org/v1.2/Workflow.html#Extensions_and_metadata).

A mapping of schema.org annotations that are commonly found in CWL documents to dxWDL `meta.details` could be written.

### CWL common Process attributes

Elements common to CWL `Workflow`, `CommandLineTool`, and `ExpressionTool`s:

| CWL      | WDL | Notes |
|----------|-----|-------|
| `id`     | `workflow` or `task` name | |
| `label`  | Could go into `meta {}` | For [dxWDL](https://github.com/dnanexus/dxWDL/blob/master/doc/ExpertOptions.md#meta-section), would map to `meta.summary`. |
| `doc`    | Could go into `meta {}` | For [dxWDL](https://github.com/dnanexus/dxWDL/blob/master/doc/ExpertOptions.md#meta-section), would map to `meta.description`. |
| `intent` | NA | Can be ignored or could be represented in WDL under `meta.intent` (but that is not a standardized key for WDL v1.0, dxWDL, Cromwell, nor the proposed WDL 2.0) |

### CWL [`CommandLineTool`](https://www.commonwl.org/v1.2/CommandLineTool.html#CommandLineTool)

Maps to a WDL Task.

| CWL            | WDL | Notes |
|----------------|-----|-------|
| `class`        | NA | always "CommandLineTool" |
| `requirements` | Varies | Some map to WDL runtime keys, some don't apply to WDL, others don't have direct mapping [See below](#CommandLineTool_requirements) |
| `hints`        | " | Same as `requirements`, but the tool can still run if these aren't satisfied |
| `baseCommand`  | The `command {}` section (along with `arguments`) | Note that a CWL tool represents a call to a single executable, where as WDL can have any number of commands in the `command {}` block; i.e. for a command block longer than a single statement, you'd have to put it into a bash script within the Docker image to call it from CWL. |
| `arguments`    | see above | An array of entries to be added to the WDL `command` section. Be careful - the result of `inputBinding`s from the inputs section may insert themselves into this list. See the `inputBinding` section below for more details. |
| `stdin`        | Done in `command {}` block e.g. using `cat` or a redirect |The path of a file to pipe into the tool being described. If not using cwltool for execution then `stdin`’ could be represented in the WDL command section by adding `< ` and the value of this field to the end of the main command block. |
| `stdout`       | `stdout()` | The path of a stdout file. See the `outputBinding` section for more information. |
| `stderr`       | `stderr()` | The path of a stderr file. See the `outputBinding` section for more information. |
| `successCodes` | NA* | * Cromwell uses [`runtime.continueOnReturnCode`](https://cromwell.readthedocs.io/en/stable/RuntimeAttributes/#continueonreturncode), WDL 2.0 specifies `runtime.returnCodes` * If not using cwltool for execution, can also be expressed as an addition to the WDL command section (basically: capture the exit code with `$?` and if it isn’t one of the `successCodes` then exit non-zero). |
| `temporaryFailCodes` and `permanentFailCodes` | NA* | * WDL 2.0 adds `maxRetries`, but that applies to all failures. DNAnexus converts return codes to different error types, and different retry behavior can be specified for each error type. Like `successCodes` they could be expressed as an addition to the WDL `command` section, if cwltool is not used for execution. |

#### [`inputs`](https://www.commonwl.org/v1.2/CommandLineTool.html#CommandInputParameter)

Maps to WDL task [`input {}`](https://github.com/openwdl/wdl/blob/main/versions/1.0/SPEC.md#task-input-declaration) section. Each entry is equivalent to a WDL `input` declaration. The actual value of the input id determined by its [`inputBinding`](#inputBinding).

| CWL              | WDL | Notes |
|------------------|-----|-------|
| `id`             | parameter name | CWL compact form uses a `?` suffix on the pattern to indicate optionality. |
| `label`          | Could go into `parameter_meta {}` | For [dxWDL](https://github.com/dnanexus/dxWDL/blob/master/doc/ExpertOptions.md#meta-section), would map to `parameter_meta.id.label`. |
| `doc`            | Could go into `parameter_meta {}` | For [dxWDL](https://github.com/dnanexus/dxWDL/blob/master/doc/ExpertOptions.md#meta-section), would map to `meta.id.description`. |
| `streamable`     | NA* | * Maps to `localizationOptional` in Cromwell and WDL 2.0, and to dxWDL’s [`stream`](https://github.com/dnanexus/dxWDL/blob/master/doc/ExpertOptions.md#streaming) hint via `parameter_meta`. |
| `format`         | NA* | The user provided vocabulary can be referenced to discover common filename extensions for recording in dxWDL style `parameter_meta.id.types` and/or `parameter_meta.id.patterns`. |
| `loadContents`   | `read_string()` | |
| `loadListing`    | NA | A similar function should be added to WDL 2.0 |
| `default`        | An input paramter can be assigned a default value. The `default` expression placeholder options can be used, but is deprecated and removed in WDL 2.0. | |
| `secondaryFiles` | NA | A [secondary file](https://www.commonwl.org/v1.2/CommandLineTool.html#SecondaryFileSchema) is a file that accompanies a main file (e.g. a .bai index file that accompanies a BAM) and is not referenced explicitly in the command line. The WDL spec [mandates](https://github.com/openwdl/wdl/blob/main/versions/development/SPEC.md#task-input-localization) that "two input files which originated in the same storage directory must also be localized into the same directory for task execution". Therefore, the secondary file has to be provided as an input and it will "just work". Structs can also be used to put main and secondary files in the same logical container. |

#### [`inputBinding`](https://www.commonwl.org/v1.2/CommandLineTool.html#CommandInputParameter)

(a.k.a [CommandLineBinding](https://www.commonwl.org/v1.2/CommandLineTool.html#CommandLineBinding); not relevant if using cwltool or other CWL aware code to render the command line).

This section describes how to determine the value of an argument and where to place it in the generated command line. In WDL, this is all done within expressions, either as definitions or within the `command {}` block.

| CWL | WDL | Notes |
|-----|-----|-------|
| `position` | implicit | Used to order the command arguments |
| `valueFrom` | implicit | If not present, then use the [table](https://www.commonwl.org/v1.2/CommandLineTool.html#CommandLineBinding) to represent the linked input textually. If present, then evaluate any CWL expressions embedded and use the result in conjunction with the following modifiers. |
| `prefix`, `separate`, `itemSeparator` | implicit - supported by `sep` and `prefix` functions/placeholder options | if `separate` is `true` (or missing) then the `prefix` goes directly as a standalone item in WDL command section prior to the result of the `valueFrom`. If `separate` is `false` then the prefix is prepended to the result of the `valueFrom`. |
| `shellQuote` | NA* | A value of `false` with `ShellCommandRequirement` in the requirements section allows for shell metacharacters to survive unquoted. If this is absent, then shell-quote the result of the remaining CommandLineBinding elements. * WDL 2.0 has the `quote` and `squote` functions, but these aren't aware of shell metacharacters. |

#### [`outputs`](https://www.commonwl.org/v1.2/CommandLineTool.html#CommandOutputParameter)

Maps to a WDL task [`output {}`](https://github.com/openwdl/wdl/blob/main/versions/1.0/SPEC.md#outputs-section) section. Nothing new here vs [input](#input).

#### [`outputBinding`](https://www.commonwl.org/v1.2/CommandLineTool.html#CommandOutputBinding)

| CWL | WDL | Notes |
|-----|-----|-------|
| `glob` | `glob()` | CWL does not require that `bash` be a part of the Docker format software container. In CWL, `glob` is defined as "using POSIX glob(3) pathname matching", so this may require changes to your execution scripts if not using cwltool or other CWL aware code. |
| `loadContents` | `read_string` | |
| `loadListing` | NA | WDL should add a function for this |
| `outputEval` | WDL expression | It may be possible to match some CWL Expressions to WDL’s `read_int()`, `read_lines()`, `read_float()`, `read_boolean()`. No implementation needed if using cwltool. |

In this `outputs` section only, two more CWL types are allowed (as a shortcut). `stdout` corresponding to WDL’s `stdout()` function. And likewise for `stderr` and WDL’s `stderr()` function. If the stdout/stdin names have been set in the CommandLineTool definition then the resulting CWL File objects will inherit those values as the `basename`. Otherwise the `basename` is random.

#### `requirements`

| CWL | WDL | Notes |
|-----|-----|-------|
| [`InlineJavascriptRequirement`](https://www.commonwl.org/v1.2/CommandLineTool.html#InlineJavascriptRequirement) | NA | Required for CWL Expressions (but not required for CWL Parameter References). |
| [`SchemaDefRequirement`](https://www.commonwl.org/v1.2/CommandLineTool.html#SchemaDefRequirement) | It should be possible to map (most) CWL schemas to WDL structs |: Defines custom named array, enum, or record types; see the above section on CWL to WDL type mapping. |
| [`DockerRequirement`](https://www.commonwl.org/v1.2/CommandLineTool.html#DockerRequirement) | `runtime.docker` | `dockerPull` or `dockerImageId`; dxWDL also supports `dockerLoad`+`dockerFile` using dx:// URLs. WDL does not specify how inputs and outputs are staged, so `dockerOutputDirectory` has no equivalent. If cwltool is not used then `dockerOutputDirectory` can be realized by ensuring that the specified directory is mounted from a writable path outside the container that has as much space available as required by `ResourceRequirements.outdirMin`. |
| [`SoftwareRequirement`](https://www.commonwl.org/v1.2/CommandLineTool.html#SoftwareRequirement) | NA | Can be turned into a WDL `runtime.docker` by using http://biocontainers.pro or http://bio.tools to do a lookup. Almost always accompanied by a `DockerRequirement` (and dxWDL will mandate a `DockerRequirement`), so it can be ignored; however, it can be used to generate [dxWDL style entries](https://github.com/dnanexus/dxWDL/blob/master/doc/ExpertOptions.md#meta-section) in `meta.details.citations`.  |
| [`InitialWorkDirRequirement`](https://www.commonwl.org/v1.2/CommandLineTool.html#InitialWorkDirRequirement) | Most of these requriements can be re-written as commands in the `command {}` section, and/or using `write_*` functions. `File` and `Directory` requirements can be re-written as WDL inputs with default values. | Specifies how the working directory is to be populated. Possibly synthesizes files on its own or via values from the `inputs` section. May not be needed if using cwltool or another CWL aware codebase. |
| [`EnvVarRequirement`](https://www.commonwl.org/v1.2/CommandLineTool.html#EnvVarRequirement) | NA | If cwltool is not used, then this can be implemented by adding `export ${envName}=${envValue}` to the beginning of the WDL `command` section. |
| [`ShellCommandRequirement`](https://www.commonwl.org/v1.2/CommandLineTool.html#ShellCommandRequirement) | | See the entry in `inputBinding`. |
| [`ResourceRequirement`](https://www.commonwl.org/v1.2/CommandLineTool.html#ResourceRequirement) | `ramMin` -> `runtime.memory` with a `MiB` suffix; `coresMin` -> `cpu`; `{tmp,out}dir{Min,Max}` -> `runtime.disks` as `local-disk`, though the values will need to be converted from MiB to GiB (the latter two are conventions in WDL 1.0 and specifications in 2.0) | Generally, WDL `runtime` requirements are minimums that must be satisfied by the runtime environment. In 2.0, WDL `hints` can also provide max values for some resources (e.g. `maxCpu`, `maxMemory`). |
| [`WorkReuse`](https://www.commonwl.org/v1.2/CommandLineTool.html#WorkReuse) | NA | Signal to allow cached results; no WDL equivalent, but can be mapped to dxWDL’s `runtime.dx_ignore_reuse` with the logic flipped. |
| [`NetworkAccess`](https://www.commonwl.org/v1.2/CommandLineTool.html#NetworkAccess) | NA | Can be mapped to [dxWDL’s `runtime.dx_access`](https://github.com/dnanexus/dxWDL/blob/master/doc/ExpertOptions.md#runtime-hints) as `{"network": “*”}`. |
| [`InplaceUpdateRequirement`](https://www.commonwl.org/v1.2/CommandLineTool.html#InplaceUpdateRequirement) | NA | Means that files are allowed to be modified in place if marked with `writable: true` via `InitialWorkDirRequirement`; therefore they cannot be mounted read-only nor can they be streamed. WDL does not specify whether input files/directories are writable; the convention is that they should be read-only and copied if they need to be modified. |
| [`ToolTimeLimit`](https://www.commonwl.org/v1.2/CommandLineTool.html#ToolTimeLimit) | NA | Can be mapped to [dxWDL’s `runtime.dx_timeout`](https://github.com/dnanexus/dxWDL/blob/master/doc/ExpertOptions.md#runtime-hints) as `{"minutes": value/60}`. |

### [`Workflow`](https://www.commonwl.org/v1.2/Workflow.html#Workflow)

Maps to a WDL `workflow`.

| CWL | WDL | Notes |
|-----|-----|-------|
| `class` | NA | always `Workflow` |
| `inputs` | `input {}` | Same as `CommandLineTool.inputs`, except that there is no `inputBinding`. |
| `outputs` | `output {}` | Same as `CommandLineTool.outputs`, except there is no `outputBinding`. Additionally, there are `outputSource` -> WDL’s output expression where the slash between the CWL step name and the step output is replaced with a period; `linkMerge` merges multiple outputs into an array -> WDL array possibly with `flatten()` function; and `pickValue` (see the discussion in `WorkflowStep.in`). |
| `requirements` | See [requirements](#requirements) | List of additional requirements for this workflow; flows down to the `CommandLineTool`s and `ExpressionTool`s within, unless there is an intervening requirement at the step level, or the `CommandLineTool`/`ExpressionTool` has the same class of requirement already. |
| `hints` | Same as task hints | If a step, sub-`Workflow`, or `CommandLineTool`/`ExpressionTool` has the same class of requirement then the class listed in the workflow `hints` is ignored in that context; otherwise the hint flows down to all sub-`Process`es (unless it isn’t applicable to that type of `Process`). |

#### Workflow `requirements`

| CWL | WDL | Notes |
|-----|-----|-------|
| `ScatterFeatureRequriement` | implicit | `zip()` and `cross()` functions can be used to satisfy most of the CWL multi-input cases. |
| `SubworkflowFeatureRequirement` | implicit | WDL subworkflows are called the same as tasks, by importing them. |
| `MultipleInputFeatureRequirement` | implicit/NA | Enables merging of workflow/step inputs. |
| `StepInputExpressionRequirement` | implicit | Enables expressions to be used to define step inputs. |

#### Workflow [`steps`](https://www.commonwl.org/v1.2/Workflow.html#WorkflowStep)

The `steps` field of a CWL `Workflow`, a list of references to other CWL `CommandLineTool`s, sub-`Workflow`s, and `ExpressionTool`s to be run; where their inputs come from; if they are scattered; and if they are run only conditionally.

| CWL | WDL | Notes |
|-----|-----|-------|
| `run` | the task/subworkflow name | The CWL Process, either as the value or a URI to it. That URI could be a relative path to the CWL description to include, or a remote HTTP(s) URI to fetch. A WDL workflow can reference an external task via imports. |
| `id` | the call name (which is either the task name or the alias) | |
| `requirements`, `hints` | NA | Additional requirements/hints can be added at the step level; WDL does not support this. If the Process already has the same type of requirements under "hints" it will be replaced with this one. 
| `when` | WDL `if result_of_when_expression then call cwl_workflow_step_id` | Note: the CWL Expression can include references to any of the inputs defined in the `in` section as `$(inputs.id)`. |
| `label`, `doc` | | While there is some place for this in WDL task meta data, WDL doesn’t have a place for step level metadata, though this could be added as `Workflow.meta.step_id.{label,description}` as non-standardized metadata. |
| `in` | call inputs | See details below. |
| `out` | All of a tasks outputs are exposed to the caller and can be referenced using dot notation | A list of output specified in the underlying Process from the `run` field to make available to other steps. Can be used as an engine optimization; if an output isn’t listed here it can be discarded (or only made available on demand by the user later). | 

##### Workflow step [input mapping](https://www.commonwl.org/v1.2/Workflow.html#WorkflowStepInput)

The `in` field of CWL `Workflow.steps.id` maps the inputs of the CWL Process in the `run` field to both CWL `Workflow` level inputs and the outputs of other CWL steps.

| CWL | WDL | Notes |
|-----|-----|-------|
| `source` | `callee_identifier.output_id` | Either a workflow level input id, or `step_id/output_id` |
|`id` | same as above | The identifier we want to provide a value for. Almost always it is an identifier from the underlying Process from the `run` field, but sometimes it isn’t so we can bring a value into scope for other reasons. |
| `default` | Use a `select_first()` expression. | The default value for this parameter to use if either there is no ‘source’ field, or the value produced by the ‘source’ is null. The default must be applied prior to scattering or evaluating ‘valueFrom’. |
| `valueFrom` | A WDL expression | either a CWL expression to evaluate in the context specified in the standard, or a constant string. If a constant string, then it is equivalent to the WDL `String self = valueFrom_value`. |
| `loadContents` | `read_string()` | Makes the first 64KiB of contents of the specified File object available to ‘valueFrom’. dxWDL imposes limitations on the file size, but this is implementation-specific. |
| `linkMerge` and `pickValue` | | see below |

A simple example:

``` cwl

class: Workflow
cwlVersion: v1.0
id: my_workflow

inputs: 
  one: string

steps:
   first:
      run: first.cwl
      in:
         { id: input1, source: one }
      out: [result]
   second:
      run: second.cwl
      in: {id: input2: source: first/result }
      out: []
outputs: []
```

``` wdl
version 1.0

import first.wdl as first
import second.wdl as second

workflow my_workflow {
  input { string one }

  call first { input1=one }
  call second { input2=first.result }
}
```

* `linkMerge`
    * If `merge_flattened` then equivalent to WDL `Array[type_of_destination] self = flatten([source])`.
    * If `merge_nested` then equivalent to WDL `Array[type_of_destination] self = [ source[0], .. source[N])`.
* `pickValue`
    * Evaluated
        1. Once all source values from upstream step or parameters are available.
        2. After linkMerge.
        3. Before scatter or valueFrom.
    * If `pickValue = first_non_null` then WDL `target_type self = select_first(self)`.
    * If `pickValue = the_only_non_null` then WDL `target_type self = if length(select_all(self)) != 1 then null else select_first(self)` but really should fail instead of null.
    * If `pickValue = all_non_null` then WDL `target_type self = select_all(self)`.

#### `scatter`

```wdl

scatter(cwl_scatter_list[0] as scatter0, .. cwl_scatter_list(N) as scatterN) {

  call step_id { input: non_scattered_inputs_spec, cwl_scatter_list[0] = scatter0, .. 

                         cwl_scatter_list[N] = scatterN}
}

```

where `non_scattered_inputs_spec` is replaced with the regular variable mappings that don’t involve scattering.

[`scatterMethod`](https://www.commonwl.org/v1.2/Workflow.html#WorkflowStep) for `dotProduct`, see the plain scatter example above:
* `scatterMethod` = `nested_crossproduct` then produce cartesian cross products with no flattening. I’m not sure if WDL has this capability as zip only works with two arrays.
* `scatterMethod` = `flat_crossproduct` is the same as `nested_crossproduct` but then flattened.


## [`ExpressionTool`](https://www.commonwl.org/v1.2/Workflow.html#ExpressionTool) and CWL Expressions

`ExpressionTool` is a CWL process that is dedicated to manipulating the intermediate object graph using ECMAScript 5.1.

All CWL ExpressionTools can be transformed to regular CommandLineTools using the script at https://github.com/common-workflow-language/cwl-utils/blob/main/cwl_utils/etools_to_clt.py

Likewise, almost all CWL expressions (`$( self / 42 )` or `${ return self / 42; }`) can be factored out as standalone CWL `ExpressionTool`s or `CommandLineTool`s using the same script linked above.

Some things that look like CWL expression in the `$( ... )` form are actually [CWL Parameter References](https://www.commonwl.org/v1.2/Workflow.html#Parameter_references) and can be converted into WDL syntax without the need for a javascript interpreter. However, it is not wrong one still uses a full javascript interpreter, just likely slower.

## [`Operation`](https://www.commonwl.org/v1.2/Workflow.html#Operation)

Part of the new abstract CWL capability. Represents a CWL Process without providing its implementation; specifically not executable. Users still might accidently try to run workflows containing one or more steps that have an `Operation` instead of another `CommandLineTool`, (sub-)`Workflow`, or `ExpressionTool`; so please check for `class: Operation` early.
