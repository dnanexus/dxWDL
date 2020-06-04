The reader is assumed to understand the [Workflow Description Language (WDL)](http://www.openwdl.org/), and have some experience using the [DNAnexus](http://www.dnanexus.com) platform.

dxWDL takes a pipeline written in WDL, and statically compiles it to an equivalent workflow on the DNAnexus platform.

- [Getting started](#getting-started)
- [Extensions](#extensions)
  * [Runtime](#runtime)
  * [Streaming](#streaming)
- [Task and workflow inputs](#task-and-workflow-inputs)
- [Task metadata](#task-metadata)
  * [meta section](#meta-section)
    + [Calling existing applets](#calling-existing-applets)
    + [Calling apps](#calling-apps)
  * [parameter_meta section](#parameter_meta-section)
  * [Runtime hints](#runtime-hints)
  * [Example task with DNAnexus-specific metadata and runtime](#example-task-with-dnanexus-specific-metadata-and-runtime)
- [Setting DNAnexus-specific attributes in extras.json](#setting-dnanexus-specific-attributes-in-extrasjson)
  * [Job reuse](#job-reuse)
  * [Delay workspace destruction](#delay-workspace-destruction)
- [Workflow metadata](#workflow-metadata)
- [Handling intermediate workflow outputs](#handling-intermediate-workflow-outputs)
  * [Use your own applet](#use-your-own-applet)
  * [Adding config-file based reorg applet at compilation time](#adding-config-file-based-reorg-applet-at-compilation-time)
- [Toplevel calls compiled as stages](#toplevel-calls-compiled-as-stages)
- [Docker](#docker)
  * [Setting a default docker image for all tasks](#setting-a-default-docker-image-for-all-tasks)
  * [Private registries](#private-registries)
  * [Storing a docker image as a file](#storing-a-docker-image-as-a-file)
- [Proxy configurations](#proxy-configurations)
- [Debugging an applet](#debugging-an-applet)
  * [Getting WDL sources](#getting-wdl-sources)
- [Recompilation](#recompilation)

# Getting started

Prerequisites: [DNAnexus platform](https://platform.dnanexus.com) account, [dx-toolkit](https://github.com/dnanexus/dx-toolkit), java 8+, python 2.7 or 3.5+.

Make sure you've installed the dx-toolkit CLI, and initialized it with `dx login`. Download the latest dxWDL compiler jar file from the [releases](https://github.com/dnanexus/dxWDL/releases) page.

## Compiling Workflow

To compile a workflow:
```console
$ java -jar dxWDL-xxx.jar compile /path/to/foo.wdl -project project-xxxx
```
This compiles `foo.wdl` to platform workflow `foo` in dx's current project and folder. The generated workflow can then be run as usual using `dx run`. For example, if the workflow takes string argument `X`, then: ``` dx run foo -i0.X="hello world" ```

Compilation can be controled with several parameters.

| Option   |  Description |
| ------   | ------------ |
| archive  | Archive older versions of applets and workflows |
| defaults | A file with default parameter settings. The syntax is Cromwell style. |
| destination | Set the output folder on the platform |
| extras   | JSON formatted file with additional options |
| force    | Overwrite existing applets/workflows if they have changed |
| inputs   | A cromwell style inputs file |
| imports  | Directory to search for imported WDL files |
| locked   | Create a locked-down workflow |
| reorg    | Move workflow intermediate results into a separate subdirectory |
| verbose  | Print detailed progress information |
| leaveWorkflowsOpen | Keep compiled workflow in `open` state |

The `-inputs` option allows specifying a Cromwell JSON
[format](https://software.broadinstitute.org/wdl/documentation/inputs.php)
inputs file. An equivalent DNAx format inputs file is generated from
it. For example, workflow
[files](https://github.com/dnanexus/dxWDL/blob/master/test/files.wdl)
has input file

```
{
  "files.f": "dx://file-F5gkKkQ0ZvgjG3g16xyFf7b1",
  "files.f1": "dx://file-F5gkQ3Q0ZvgzxKZ28JX5YZjy",
  "files.f2": "dx://file-F5gkPXQ0Zvgp2y4Q8GJFYZ8G"
}
```

The command

```console
java -jar dxWDL-0.44.jar compile test/files.wdl -project project-xxxx -inputs test/files_input.json
```

generates a `test/files_input.dx.json` file that looks like this:
```
{
  "f": {
    "$dnanexus_link": "file-F5gkKkQ0ZvgjG3g16xyFf7b1"
  },
  "f1": {
    "$dnanexus_link": "file-F5gkQ3Q0ZvgzxKZ28JX5YZjy"
  },
  "f2": {
    "$dnanexus_link": "file-F5gkPXQ0Zvgp2y4Q8GJFYZ8G"
  }
}
```

The workflow can then be run with the command:

```console
$ dx run files -f test/files_input.dx.json
```

The `-defaults` option is similar to `-inputs`. It takes a JSON file with key-value pairs,
and compiles them as defaults into the workflow. If the `files.wdl` worklow is compiled with
`-defaults` instead of `-inputs`

```console
$ java -jar dxWDL-0.44.jar compile test/files.wdl -project project-xxxx -defaults test/files_input.json
```

It can be run without parameters, for an equivalent execution.

```console
$ dx run files
```

The `extras` command line option allows, for example, the Cromwell feature of setting the
default runtime attributes of a task.

If this is file `extraOptions.json`:

```
{
    "default_runtime_attributes" : {
      "docker" : "quay.io/encode-dcc/atac-seq-pipeline:v1"
    }
}
```

Then adding it to the compilation command line will add the `atac-seq` docker image to all
tasks by default.

```console
$ java -jar dxWDL-0.44.jar compile test/files.wdl -project project-xxxx -defaults test/files_input.json -extras extraOptions.json
```

## Describe WDL workflow to obtain execution tree

You can describe a dnanexus workflow that was compiled by dxWDL to get an execution tree presentating the workfow.w.
The execution tree will include information on the executables in the workflow (applets and subworkflows).
By default, the execution tree is return as JSON. You can supply a `--pretty` flag to return a pretty print.

To obtain execution tree from a dxWDL compiled workflow:

1. JSON - [example](./examples/four_levels.exectree.json)

```bash
java -jar dxWDL-v1.46.5.jar describe <workflow_id>
```

2. prettyPrint - [example](./examples/four_levels.exectree.pretty.txt)

```bash
java -jar dxWDL-v1.46.5.jar describe <workflow_id> -pretty
```

# Extensions

## Runtime

A task declaration has a runtime section where memory, cpu, and disk
space can be specified. Based on these attributes, an instance type is chosen by
the compiler. If you wish to choose an instance type from the
[native](https://documentation.dnanexus.com/developer/api/running-analyses/instance-types)
list, this can be done by specifying the `dx_instance_type` key
instead. For example:

```
runtime {
   dx_instance_type: "mem1_ssd2_x4"
}
```

If you want an instance that has a GPU chipset, set the `gpu` attribute to true. For example:
```
runtime {
   memory: "4 GB"
   cpu : 4
   gpu : true
}
```

## Streaming

Normally, a file used in a task is downloaded to the instance, and
then used locally (*locallized*). If the file only needs to be
examined once in sequential order, then this can be optimized by
streaming instead. The Unix `cat`, `wc`, and `head` commands are of
this nature. To specify that a file is to be streamed, mark it as such
in the `parameter_meta` section. For example:

```wdl
task head {
    File in_file
    Int num_lines

    parameter_meta {
        in_file : "stream"
    }
    command {
        head -n ${num_lines} ${in_file}
    }
    output {
        String result = read_string(stdout())
    }
}
```

File streaming is an optimization, and there are limiting rules to its
correct usage. The file must be accessed only once, in sequential
order, from the beginning. It need not be read to the end. If the task
does not keep this contract, it could fail in unexpected ways.

Currently the download tool (Dx Download Agent) used by dxWDL does not handle symliinked data transfer. If the streaming method does not perform well for the tool inside your workflow, you may consider using the file ID of the input data as string and use `dx download` in the command section to download the symlinked data to the worker for processing. Example:

```wdl
task download_test {

    String bam

    command {
        dx download ${bam} -o - > ~/file.bam
        samtools index ~/file.bam
    }
    output {
        File result = "..."
    }
}
```

Some tasks have empty command sections. For example, the `fileSize`
task (below) calculates the size of a file, but does not need to
download it.  In such cases, the input files are downloaded lazily,
only if their data is accessed.

```wdl
task fileSize {
    File in_file

    command {}
    output {
        Float num_bytes = size(in_file)
    }
}
```

# Task and workflow inputs

WDL assumes that a task declaration can be overriden
by the caller, if it is unassigned, or assigned to a constant.

```wdl
task manipulate {
  Int x
  Int y = 6
  Int z = y + x
  ...
}
```

In the `manipulate` task `x` and `y` are compiled to applet inputs,
where `y` has a default value (6). This allows the applet caller to
override them. Declaration `z` is not considered an input, because it
is assigned to an expression.

In a workflow, similarly to a task, a declaration is considered an
input if it is unassigned or or assigned to a constant. For example,
workflow `foo` has three inputs: `ref_genome`, `min_coverage`, and
`config`. Variable `max_coverage` is not compiled into an input
because it is assigned to an expression. Note that `config` is an
input, even though it is located in the middle of the workflow.

```wdl
workflow foo {
    File ref_genome
    Float min_coverage = 0.8
    Float max_coverage = min_coverage + 0.1

    call GetVersion
    scatter (i in [1,2,3]) {
        call RandCheck { input: ref=ref_genome, seed=i }
    }

    String config = "test"
    ...
}
```

WDL allows leaving required call inputs unassigned, and
specifying them from the input file. For example, workflow `math`
calls task `add`, but does not specify argument `b`. It can then
be specified from the input file as follows: `{ "math.add.b" : 3}`.

```wdl
task add {
    Int a
    Int b
    output {
        Int result = a + b
    }
}

workflow math {
    call add { input: a = 3 }
    output {
       add.result
    }
}
```

Currently, dxWDL does not support this feature. However, there is a [suggestion](MissingCallArguments.md) for limited support.

# Task metadata

A WDL task has two sections where metadata can be specified:

* meta: Provides overall metadata about the task
* parameter_meta: Provides metadata for each of the input parameters

Both of these sections allow arbitrary keys and values; unrecognized keys must be ignored by the workflow engine. dxWDL recognized specific keys in each section that are used when generating the native DNAnexus applets. The purpose of these keys is to provide the same information that can be specified in the [dxapp.json](https://documentation.dnanexus.com/developer/apps/app-metadata) file.

## meta section

The following keys are recognized:

* `title`: A short title for the applet. If not specified, the task name is used as the title.
* `summary`: A short description of the applet. If not specified, the first line of the description is used (up to 50 characters or the first period, whichever comes first).
* `description`: A longer description of the applet.
* `developer_notes`: Notes specifically for developers of the task.
* `types`: An array of DNAnexus [types](https://documentation.dnanexus.com/developer/api/data-object-lifecycle/types).
* `tags`: An array of strings that will be added as tags on the generated applet.
* `properties`: A hash of key-value pairs that will be added as properties on the generated applet. Both keys and values must be strings.
* `details`: An object with an arbitrary set of details about the applet. The following keys are specifically recognized and used by the platform:
  * `advancedInputs`
  * `citations`
  * `contactEmail`
  * `contactOrg`
  * `contactUrl`
  * `exampleProject`
  * `repoUrl`
  * `upstreamLicenses`
  * `upstreamUrl`
  * `upstreamVersion`
  * `whatsNew`: The task's change log. There are two different formats that are accepted:
    * A (possibly Markdown-formatted) string
    * An array of versions, where each version is a hash with two keys: `version`, a version string, and `changes`, an array of change description strings. This object will be formatted into a Markdown string upon compilation.

The following keys are also recognized but currently unused, as they only apply to DNAnexus Apps (not Applets):

* `categories`: A list of DNAnexus [categories](https://documentation.dnanexus.com/developer/apps/app-metadata#categories-user-browseable-categories)
* `open_source`: Whether the generated app should be open-source
* `version`: The app version

### Calling existing applets

Sometimes, it is desirable to call an existing dx:applet from a WDL workflow. For example, when porting a native workflow, we can leave the applets as is, without rewriting them in WDL. The `dxni` subcommand, short for *Dx Native Interface*, is dedicated to this use case. It searchs a platform folder and generates a WDL wrapper task for each applet. For example, the command:

```console
$ java -jar dxWDL.jar dxni --project project-xxxx --folder /A/B/C --output dx_extern.wdl
```

will find native applets in the `/A/B/C` folder, generate tasks for
them, and write to local file `dx_extern.wdl`. If an
applet has the `dxapp.json` signature:

```
{
  "name": concat,
  "inputSpec": [
    {
      "name": "a",
      "class": "string"
    },
    {
      "name": "b",
      "class": "string"
    }
  ],
  "outputSpec": [
    {
      "name": "result",
      "class": "string"
    }]
}
```

The WDL definition file will be:

```wdl
task concat {
  String a
  String b
  command {}
  output {
    String c = ""
  }
  meta {
    type: "native"
    id: "applet-xxxx"
  }
}
```

The meta section includes the applet-id, which will be called at runtime. A WDL file can
call the `concat` task as follows:

```wdl
import "dx_extern.wdl" as lib

workflow w {
  call lib.concat as concat {
    input: a="double", b="espresso"
  }
  output {
    concat.c
  }
}
```

### Calling apps

To call apps instead of applets, use

```console
$ java -jar dxWDL.jar dxni -apps -o my_apps.wdl
```

The compiler will search for all the apps you can call, and create WDL
tasks for them.

## parameter_meta section

The [WDL Spec](https://github.com/openwdl/wdl/blob/master/versions/1.0/SPEC.md#parameter-metadata-section) defines a `parameter_meta` section that may contain key value pairs to assoicate metadata with input and output variables. Currently, the following keywords are supported:

- `stream`, indicates whether or not an input file should be streamed. See [here](#Streaming) for more details
- Direct mappings to [inputSpec and outputSpec keywords in dxapp.json](https://documentation.dnanexus.com/developer/api/running-analyses/io-and-run-specifications):
  - `help` - `description` is also accepted as an alias for `help`; if the parameter definition is a string rather than a hash, the string is used as `help`.
  - `group` - parameter grouping (used in the DNAnexus web UI).
  - `label` - human-readable label for the parameter (used in the DNAnexus web UI).
  - `patterns` - accepted filename patterns (applies to `File`-type parameters only).
  - `choices` - allowed parameter values; currently, this is limited to primitive (`String`, `Int`, `Float`, `Boolean`) and `File` types parameters (and `Array`s of these types), i.e. it is not allowed for `Map` or `Struct` parameters.
  - `suggestions` - suggested parameter values; currently has the same limitations as `choices`.
  - `dx_type` - maps to the `type` field in dxapp.json; can be either a `String` value or a boolean "expression" (see example below). Applies to `File`-type parameters only.
  - `default` - a default value for the parameter. This is ignored if the parameter's default value is defined in the `inputs` section.

Although the WDL spec indicates that the `parameter_meta` section should apply to both input and output variables, WOM currently only maps the parameter_meta section to the input parameters.

## Runtime hints

There are several parameters affecting the runtime environment that can be specified in the dxapp.json file:

* `executionPolicy`: Specifies when to try to automatically restart failed jobs, and how many times
* `timeoutPolicy`: Specifies the maximum amount of time the job can run
* `access`: Specifies which resources the applet can access
* `ignoreReuse`: Specifies whether to allow the outputs of the applet to be reused

These attributes can be specified in the `runtime` section of the WDL task, but their representation there is slightly different than in dxapp.json. Also note that the runtime section is different than the metadata section when it comes to attribute values - specifically, object values must be prefixed by the `object` keyword, and map values must have their keys in quotes.

* `dx_restart`: Either an integer value indicating the number of times to automatically restart regardless of the failure reason, or an object value with the following keys:
  * `max`: Maximum number of restarts
  * `default`: Default number of restarts for any error type
  * `errors`: Mapping of [error types](https://documentation.dnanexus.com/developer/api/running-analyses/io-and-run-specifications#run-specification) to number of restarts
* `dx_timeout`: Either a string value that specifies days, hours, and/or minutes in the format "1D6H30M" or an object with at least one of the keys `days`, `hours`, `minutes`.
* `dx_access`: An object with any of the keys:
  * `network`: An array of domains to which the app has access, or "*" for all domains
  * `project`: The maximum level of access the applet has to the host project - a string with any DNAnexus access level
  * `allProjects`: The maximum level of access the applet has to all projects
  * `developer`: Boolean - whether the applet is a developer, i.e. can create new applets
  * `projectCreation`: Boolean - whether the applet can create new projects
* `dx_ignore_reuse`: Boolean - whether to allow the outputs of the applet to be reused
* `dx_instance_type`: String - DNAnexus instance type which the applet will use.

## Example tasks with DNAnexus-specific metadata and runtime

### Example 1: grep for pattern in file

```wdl
version 1.0

task cgrep {
    input {
        String pattern
        File in_file
        Int? max_results
    }
    Int actual_max_results = select_first([max_results, 3])

    meta {
        title: "Search in File"
        tags: ["search", "grep"]
        details: {
          whatsNew: [
            { version: "1.1", changes: ["Added max_results", "Switched to WDL v1.0"]},
            { version: "1.0", changes: ["Initial release"]}
          ]
        }
    }

    parameter_meta {
        in_file: {
          help: "The input file to be searched",
          group: "Basic",
          patterns: ["*.txt", "*.tsv"],
          dx_type: { and: [ "fastq", { or: ["Read1", "Read2"] } ] },
          stream: true
        }
        pattern: {
          help: "The pattern to use to search in_file",
          group: "Advanced"
        }
        max_results: {
          help: "Maximum number of results to return",
          choices: [1, 2, 3],
          default: 3
        }
    }

    command <<<
        grep -m~{actual_max_results} '~{pattern}' ~{in_file} | wc -l
        cp ~{in_file} out_file
    >>>

    output {
        Int count = read_int(stdout())
        File out_file = "out_file"
    }

    runtime {
      docker: "ubuntu:latest"
      dx_instance_type: "mem1_ssd1_v2_x8"
      dx_ignore_reuse: true
      dx_restart: object {
          default: 1,
          max: 5,
          errors: {
              "UnresponsiveWorker": 2,
              "ExecutionError": 2,
          }
      }
      dx_timeout: "12H30M"
      dx_access: object {
          network: ["*"],
          developer: true
      }
    }
}
```

### Example 2: alignment with BWA-MEM

```wdl
version 1.0

task bwa_mem {
  input {
    String sample_name
    File fastq1_gz
    File fastq2_gz
    File genome_index_tgz
    Int min_seed_length = 19
    String? read_group
    String docker_image = "broadinstitute/baseimg"
    Int cpu = 4
    Int memory_gb = 8
    Int? disk_gb
  }

  String genome_index_basename = basename(genome_index_tgz, ".tar.gz")
  String actual_read_group = select_first([
    read_group,
    "@RG\\tID:${sample_name}\\tSM:${sample_name}\\tLB:${sample_name}\\tPL:ILLUMINA"
  ])
  Int actual_disk_gb = select_first([
    disk_gb,
    ceil(2 * (size(genome_index_tgz, "G") + size(fastq1_gz, "G") + size(fastq2_gz, "G")))
  ])

  command <<<
  set -eux
  tar xzvf ~{genome_index_tgz}
  bwa mem \
    -M \
    -t ~{cpu} \
    -R "~{actual_read_group}" \
    -k ~{min_seed_length} \
    ~{genome_index_basename}.fa \
    ~{fastq1_gz} ~{fastq2_gz} | \
    samtools view -Sb > ~{sample_name}.bam
  >>>

  output {
    File bam = "${sample_name}.bam"
  }

  runtime {
    docker: docker_image
    cpu: "${cpu}"
    memory: "${memory_gb} GB"
    disks: "local-disk ${actual_disk_gb} SSD"
    dx_timeout: "1D"
    dx_restart: {
      "max": 3
    }
  }

  meta {
    title: "BWA-MEM"
    description: "Align paired-end reads using BWA MEM"
    details: {
      upstreamLicenses: "GPLv3"
    }
  }

  parameter_meta {
    sample_name: {
      label: "Sample Name",
      help: "Name of the sample; used to prefix output files"
    }
    fastq1_gz: {
      label: "FASTQ 1 (gzipped)",
      description: "Gzipped fastq file of first paired-end reads",
      stream: true
    }
    fastq2_gz: {
      label: "FASTQ 2 (gzipped)",
      description: "Gzipped fastq file of second paired-end reads",
      stream: true
    }
    genome_index_tgz: {
      label: "Genome Index (.tgz)",
      description: "Tarball of the reference genome and BWA index",
      stream: true
    }
    min_seed_length: {
      label: "Minimum Seed Length",
      help: "Matches shorter than INT will be missed.",
      group: "Advanced",
      default: 19
    }
    read_group: {
      label: "Read Group",
      help: "(Optional) the read group to add to aligned reads",
      group: "Advanced"
    }
    docker_image: {
      label: "Docker Image",
      help: "Name of the docker image to use",
      group: "Resources",
      default: "broadinstitute/baseimg"
    }
    cpu: {
      label: "CPUs",
      help: "Minimum number of CPUs to use",
      group: "Resources",
      default: 4
    }
    memory_gb: {
      label: "Memory (GB)",
      help: "Minimum amount of memory required",
      group: "Resources",
      default: 8
    }
    disk_gb: {
      label: "Disk Space (GB)",
      help: "Minimum amount of disk space required (in GB); by default this is calculated from the inputs",
      group: "Resources"
    }
  }
}
```

\* Note the comma seperating the members of the objects within meta and paramter_meta

# Setting DNAnexus-specific attributes in extras.json

When writing a dnanexus applet the user can specify options through the [dxapp.json](https://documentation.dnanexus.com/developer/apps/app-metadata#annotated-example) file. The dxWDL equivalent is the *extras* file, specified with the `extras` command line option. The extras file has a `default_task_dx_attributes` section where runtime specification, timeout policies, and access control can be set.

```
{
  "default_task_dx_attributes" : {
    "runSpec": {
        "executionPolicy": {
          "restartOn": {
            "*": 3
          }
        },
        "timeoutPolicy": {
          "*": {
            "hours": 12
          }
        },
        "access" : {
          "project": "CONTRIBUTE",
          "allProjects": "VIEW",
          "network": [
            "*"
          ],
          "developer": true
        }
      }
  }
}
```

In order to override the defaults for specific tasks, you can add the `per_task_dx_attributes` section. For example

```
{
  "per_task_dx_attributes" : {
    "Add": {
      "runSpec": {
        "timeoutPolicy": {
          "*": {
            "minutes": 30
          }
        }
      }
    },
    "Inc" : {
      "runSpec": {
        "timeoutPolicy": {
          "*": {
            "minutes": 30
          }
        },
        "access" : {
          "project": "UPLOAD"
        }
      }
    }
  }
}
```

will override the default timeout for tasks `Add` and `Inc`. It will also provide `UPLOAD` instead of `VIEW` project access to `Inc`.

You are also able add citations or licenses information using for each task at the `per_task_dx_attributes` section. For example

```
{
  "per_task_dx_attributes" : {
    "Add": {
      "runSpec": {
        "timeoutPolicy": {
          "*": {
             "minutes": 30
          }
        }
      },
      "details": {
        "upstreamProjects": [
          {
            "name": "GATK4",
            "repoUrl": "https://github.com/broadinstitute/gatk",
            "version": "GATK-4.0.1.2",
            "license": "BSD-3-Clause",
            "licenseUrl": "https://github.com/broadinstitute/LICENSE.TXT",
            "author": "Broad Institute"
          }
        ]
      }
    },
  }
}
```

Note that `details` specified in `per_task_dx_attributes` override those that are set in the task's `meta` section.

## Job reuse

By default, job results are [reused](https://documentation.dnanexus.com/user/running-apps-and-workflows/job-reuse). This is an optimization whereby when a job is run a second time, the results from the previous execution are returned, skipping job execution entirely. Sometimes, it is desirable to disable this behavior. To do so use:

```
{
  "ignoreReuse" : true
}
```

## Delay workspace destruction

By default, temporary workspaces hold the results of executed workflows and applets. Normally, these are garbage collected by the system. If you wish to leave them around longer for debugging purposes, please use:

```
{
  "delayWorkspaceDestruction" : true
}
```

This will be passed down through the entire workflow, sub-workflows, and tasks. Workspaces will remain intact for 72 hours. This is a runtime flag, so you will need to run the toplevel workflow with that flag:

```
dx run YOUR_WORKFLOW --delay-workspace-destruction
```

# Workflow metadata

Similar to tasks, workflows can also have `meta` AND `parameter_meta` sections that contain arbitrary workflow-level metadata. dxWDL recognizes the following `meta` attributes and uses them when generating the native DNAnexus workflow:

* `title`: A short title for the workflow. If not specified, the task name is used as the title.
* `summary`: A short description of the workflow. If not specified, the first line of the description is used (up to 50 characters or the first period, whichever comes first).
* `description`: A longer description of the workflow.
* `types`: An array of DNAnexus [types](https://documentation.dnanexus.com/developer/api/data-object-lifecycle/types).
* `tags`: An array of strings that will be added as tags on the generated applet.
* `properties`: A hash of key-value pairs that will be added as properties on the generated applet. Both keys and values must be strings.
* `details`: A hash of workflow details. The only key that is specifically recogized is `whatsNew`, and the formatting is handled for workflows the same way as it is for tasks.

The workflow `parameter_meta` section supports the same attributes as the task `parameter_meta` section.

# Handling intermediate workflow outputs

A workflow may create a large number of files, taking up significant
disk space, and incurring storage costs. Some of the files are
workflow outputs, but many of them may be intermediate results that
are not needed once the workflow completes. By default, all outputs
are stored in one platform folder. With the `--reorg` flag, the
intermediate results are moved into a subfolder named
"intermediate". This is achieved by adding a stage to the workflow
that reorganizes the output folder, it uses `CONTRIBUTE` access to
reach into the parent project, create a subfolder, and move files into
it.

## Use your own applet

You may want to use a different applet than the one provided with `--reorg`. To
do that, write a native applet, and call it at the end your workflow.

Writing your own applet for reorganization purposes is tricky. If you are not careful,
it may misplace or outright delete files. The applet:
1. requires `CONTRIBUTE` project access, so it can move files and folders around.
2. has to be idempotent, so that if the instance it runs on crashes, it can safely restart.
3. has to be careful about inputs that are *also* outputs. Normally, these should not be moved.
4. should use bulk object operations, so as not to overload the API server.

## Adding config-file based reorg applet at compilation time

In addition to using `--reorg` flag to add the reorg stage, you may also add a custom reorganization applet that takes an optional input by declaring a "custom-reorg" object in the JSON file used as parameter with `-extras`

The  "custom-reorg" object has two properties in extra.json:
    # app_id: reorg applet id
    # conf: auxiliary configuration

The optional input file can be used as a configuration file for the reorganization process.

For example:

```

{
  "custom-reorg" : {
    "app_id" : "applet-12345678910",
    "conf" : "dx://file-xxxxxxxx"
  }
}

# if you do not wish to include an additional config file, please set the "conf" to `null`
{
  "custom-reorg" : {
    "app_id" : "applet-12345678910",
    "conf" : null
  }
}

```

The config-file based reorg applet needs to have the following specs as inputs.

`reorg_conf___` and `reorg_status___`:

```json
{
  "inputSpec": [
    {
      "name": "reorg_conf___",
      "label": "Auxiliary config input used for reorganisation.",
      "help": "",
      "class": "file",
      "patterns": ["*"],
      "optional": true
    },
    {
      "name": "reorg_status___",
      "label": "A string from output stage that act as a signal to indicate the workflow has completed.",
      "help": "",
      "class": "string",
      "optional": true
    }
  ]
}
```

When compiling a workflow with a custom-reorg applet declared with `-extras` JSON, a string variable `reorg_status___` with the value of `completed` will be included in the output stage.

The `reorg_status___` is used to act as a dependency to signal that the workflow has completed.

For an example use case of a configuration based custom reorg applet, please refer to [CustomReorgAppletExample.md](CustomReorgAppletExample.md).

# Top-level calls compiled as stages

If a workflow is compiled in unlocked mode, top level calls with no
subexpressions are compiled directly to dx:workflow stages. For
example, in workflow `foo` call `add` is compiled to a dx:stage.
`concat` has a subexpression, and `check` is not a top level call; they
will be compiled to dx:applets.

```wdl
workflow foo {
    String username
    Boolean flag

    call add
    call concat {input: x="hello", y="_" + username }

    if (flag) {
        call check {input:  factor = 1 }
    }
}

task add {
    Int a
    Int b
    command {}
    output { Int result = a + b }
}

task concat {
   String s1
   String s2
   command {}
   output { String result = s1 + s2 }
}

task check {
   Int factor = 3
   ...
}
```

When a call is compiled to a stage, missing arguments are transformed
into stage inputs. The `add` stage will have compulsory integer inputs
`a` and `b`.

For an in depth discussion, please see [Missing Call Arguments](MissingCallArguments.md).

# Docker

As of release 0.80, we moved to using docker, instead of
[dx-docker](https://documentation.dnanexus.com/developer/apps/dependency-management/using-docker-images). `dx-docker`
is deprecated, although you can still use it with the `--useDxDocker` command line flag.

## Setting a default docker image for all tasks

Sometimes, you want to use a default docker image for tasks.
The `extras` commad line flag can help achieve this. It takes a JSON file
as an argument. For example, if `taskAttrs.json` is this file:
```
{
    "default_runtime_attributes" : {
      "docker" : "quay.io/encode-dcc/atac-seq-pipeline:v1"
    }
}
```

Then adding it to the compilation command line will add the `atac-seq` docker image to all tasks by default.

```console
$ java -jar dxWDL-0.44.jar compile files.wdl -project project-xxxx -defaults files_input.json -extras taskAttrs.json
```

## Private registries

If your images are stored in a private registry, add its information
to the extras file, so that tasks will be able to pull images from it.
For example:

```
{
  "docker_registry" : {
      "registry" : "foo.acme.com",
       "username" : "perkins",
       "credentials" : "dx://CornSequencing:/B/creds.txt"
  }
}
```

will setup the `foo.acme.com` registry, with user `perkins`. The
credentials are stored in a platform file, so they can be replaced
without recompiling. Care is taken so that the credentials never
appear in the applet logs. Compiling a workflow with this
configuration sets it to use native docker, and all applets are given
the `allProjects: VIEW` permission. This allows them to access the
credentials file, even if it is stored on a different project.

## Storing a docker image as a file

Normally, [docker](https://www.docker.com/) images are public, and
stored in publicly available web sites. This enables reproducibility
across different tools and environments. However, if you have a
docker image that you wish to store on the platform,
you can do `docker save`, followed by uploading the tar ball to platform file `file-xxxx`. Then, specify the docker attribute in the runtime section as
`dx://file-xxxx`. Paths or file ids can be used, for example:
```
runtime {
   docker: "dx://GenomeSequenceProject:/A/B/myOrgTools"
}

runtime {
   docker: "dx://file-xxxx"
}

runtime {
   docker: "dx://project-xxxx:file-yyyy"
}
```

# Proxy configurations

Some organizations place a proxy between internal machines and
external hosts. This is done for security, auditing, and caching
purposes. In this case, the compiler cannot contact the dnanexus API
servers, unless is routes its requests through the proxy. Do
achieve this, set the environment variable `HTTP_PROXY` (or
`HTTPS_PROXY`) to point to the proxy. For example, if you perform the
following on the command line shell:

```bash
$ export HTTP_PROXY = proxy.acme.com:8080
$ java -jar dxWDL.jar compile ...
```

the compiler will send all requests through the machine `proxy.acme.com` on port `8080`.

If an a proxy with NTLM authentication is used, the following configuration is required:

```
$ export HTTP_PROXY_METHOD=ntlm
$ export HTTP_PROXY_DOMAIN = acme.com
$ export HTTP_PROXY = https://john_smith:welcome1@proxy.acme.com:8080
$ java -jar dxWDL.jar compile ...
```

# Debugging an applet

If you build an applet on the platform with dxWDL, and want to inspect
it, use: ```dx get --omit-resources <applet path>```. This will
refrain from downloading the large resource files that go into the
applet.

## Getting WDL sources

Compiled workflows and tasks include the original WDL source code in
the details field. For example, examine workflow `foo` that was
compiled from `foo.wdl`.  The platform object `foo` includes a details
field that contains the WDL source, in compressed, uuencoded
form. To extract it you can do:

```
dx describe /builds/1.02/applets/hello --json --details | jq '.details | .womSourceCode' | sed 's/"//g' | base64 --decode | gunzip
```

# Recompilation

Any significant WDL workflow is compiled into multiple DNAx applets
and workflows. Naively, any modification to the WDL source would
necessitate recompilation of all the constituent objects, which is
expensive. To optimize this use case, all generated platform objects are
checksumed. If a dx:object has not changed, it is not recompiled, and
the existing version can be used. The checksum covers the WDL source
code, the DNAx runtime specification, and any other attributes. There
are two exceptions: the project name, and the folder. This allows
moving WDL workflows in the folder hierarchy without recompilation.
