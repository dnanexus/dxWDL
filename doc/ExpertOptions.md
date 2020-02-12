The reader is assumed to understand the
[Workflow Description Language (WDL)](http://www.openwdl.org/), and have
some experience using the [DNAnexus](http://www.dnanexus.com) platform.

*dxWDL* takes a bioinformatics pipeline written in WDL, and statically
compiles it to an equivalent workflow on the DNAnexus platform.


# Getting started
Prerequisites: DNAnexus platform account, dx-toolkit, java 8+, python 2.7.

Make sure you've installed the dx-toolkit CLI, and initialized it with
`dx login`. Download the latest compiler jar file from the
[releases](https://github.com/dnanexus/dxWDL/releases) page.

To compile a workflow:
```console
$ java -jar dxWDL-xxx.jar compile /path/to/foo.wdl -project project-xxxx
```
This compiles `foo.wdl` to platform workflow `foo` in dx's
current project and folder. The generated workflow can then be run as
usual using `dx run`. For example, if the workflow takes string
argument `X`, then: ``` dx run foo -i0.X="hello world" ```

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

# Extensions

## Runtime
A task declaration has a runtime section where memory, cpu, and disk
space can be specified. Based on these attributes, an instance type is chosen by
the compiler. If you wish to choose an instance type from the
[native](https://wiki.dnanexus.com/api-specification-v1.0.0/instance-types)
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

Currently, dxWDL does not support this feature. However, there is a [suggestion](MissingCallArguments.md)
for limited support.

# parameter_meta section

The [WDL Spec](https://github.com/openwdl/wdl/blob/master/versions/1.0/SPEC.md#parameter-metadata-section) defines a `parameter_meta` section that may contain key value pairs to assoicate metadata with input and output variables. Currently, the following keywords are supported:

- `stream`, indicates whether or not an input file should be streamed. See [here](#Streaming) for more details
- Direct mappings to [inputSpec and outputSpec keywords in dxapp.json](https://documentation.dnanexus.com/developer/api/running-analyses/io-and-run-specifications):
  - `group`
  - `help`
  - `label`
  - `patterns`
<!--
  - `choices`
  - `suggestions`
  - `dx_type` (maps to the `type` field in dxapp.json)
-->

Although the WDL spec indicates that the `parameter_meta` section should apply to both input and output variables, WOM currently only maps the parameter_meta section to the input parameters.

## Example parameter_meta app

```wdl
task cgrep {
    input {
        String pattern
        File in_file
    }
    parameter_meta {
        in_file: {
          help: "The input file to be searched",
          patterns: ["*.txt", "*.tsv"],
          stream: true
        }
        pattern: {
          help: "The pattern to use to search in_file"
        }
    }
    command {
        grep '${pattern}' ${in_file} | wc -l
        cp ${in_file} out_file
    }
    output {
        Int count = read_int(stdout())
        File out_file = "out_file"
    }
}
```

* Note the comma seperating the members of the object for `in_file`

# Calling existing applets

Sometimes, it is desirable to call an existing dx:applet from a WDL
workflow. For example, when porting a native workflow, we can leave
the applets as is, without rewriting them in WDL. The `dxni`
subcommand, short for *Dx Native Interface*, is dedicated to this use
case. It searchs a platform folder and generates a WDL wrapper task for each
applet. For example, the command:

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

## Calling apps

To call apps instead of applets, use

```console
$ java -jar dxWDL.jar dxni -apps -o my_apps.wdl
```

The compiler will search for all the apps you can call, and create WDL
tasks for them.


## Debugging an applet

If you build an applet on the platform with dxWDL, and want to inspect
it, use: ```dx get --omit-resources <applet path>```. This will
refrain from downloading the large resource files that go into the
applet.


## Setting dnanexus specific attributes for tasks

When writing a dnanexus applet the user can specify options through
the [dxapp.json](https://wiki.dnanexus.com/dxapp.json) file. The dxWDL
equivalent is the *extras* file, specified with the
`extras` command line option. The extras file has a `default_task_dx_attributes`
section where runtime specification, timeout policies, and access control can
be set.

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

In order to override the defaults for specific tasks, you can add the `per_task_dx_attributes`
section. For example

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

will override the default timeout for tasks `Add` and `Inc`. It will also provide
`UPLOAD` instead of `VIEW` project access to `Inc`.

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
This will be passed down through the entire workflow, sub-workflows, and tasks. Workspaces will remain intact for 72 hours.
This is a runtime flag, so you will need to run the toplevel workflow with that flag:

```
dx run YOUR_WORKFLOW --delay-workspace-destruction
```

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
In addition to using `--reorg` flag to add the reorg stage, you may also add a custom reorganization applet that takes an optional input
by declaring a "custom-reorg" object in the JSON file used as parameter with `-extras`

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

When compiling a workflow with a custom-reorg applet declared with `-extras` JSON,
a string variable `reorg_status___` with the value of `completed` will be included in the output stage.

The `reorg_status___` is used to act as a dependency to signal that the workflow has completed.

For an example use case of a configuration based custom reorg applet, please refer to [CustomReorgAppletExample.md](CustomReorgAppletExample.md)
# Toplevel calls compiled as stages

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
[dx-docker](https://wiki.dnanexus.com/Developer-Tutorials/Using-Docker-Images). `dx-docker`
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

Then adding it to the compilation command line will add the `atac-seq` docker image to all
tasks by default.
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
you can do `docker save`, followed by uploading the tar ball to platform file `file-xxxx`.
Then, specify the docker attribute in the runtime section as
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

# Getting WDL sources

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
