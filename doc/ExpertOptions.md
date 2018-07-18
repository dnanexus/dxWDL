# Advanced options

The reader is assumed to understand the
[Workflow Description Language (WDL)](http://www.openwdl.org/), and have
some experience using the [DNAnexus](http://www.dnanexus.com) platform.

*dxWDL* takes a bioinformatics pipeline written in WDL, and statically
compiles it to an equivalent workflow on the DNAnexus platform.


## Getting started
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
```json
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
```json
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
```json
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

## Extensions

A task declaration has a runtime section where memory, cpu, and disk
space can be specified. Based on these attributes, an instance type is chosen by
the compiler. If you wish to choose an instance type from the
[native](https://wiki.dnanexus.com/api-specification-v1.0.0/instance-types)
list, this can be done by specifying the `dx_instance_type` key
instead. For example:

```json
runtime {
   dx_instance_type: "mem1_ssd2_x4"
}
```

Normally, a file used in a task is downloaded to the instance, and
then used locally (*locallized*). If the file only needs to be
examined once in sequential order, then this can be optimized by
streaming instead. The Unix `cat`, `wc`, and `head` commands are of this
nature. To specify that a file is to be streamed, mark it as such in
the `parameter_meta` section. For example:

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


## Task and workflow inputs

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

## Calling existing applets

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

```json
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

## Using a docker image on the platform

Normally, docker images are public, and stored in publicly available
web sites. This enables reproducibility across different tools and
environments. However, if you have private docker image that you wish
to store on the platform, dx-docker
[create-asset](https://wiki.dnanexus.com/Developer-Tutorials/Using-Docker-Images)
can be used. In order to use a private image, you can specify the
docker attribute in the runtime section as:
`dx://project-id:/image-name`.

For example:
```json
runtime {
   docker: "dx://GenomeSequenceProject:/A/B/myOrgTools"
}

runtime {
   docker: "dx://project-xxxx:record-yyyy"
}
```

## Setting a default docker image for all tasks

Sometimes, you want to use a default docker image for tasks.
The `extras` commad line flag can help achieve this. It takes a JSON file
as an argument. For example, if `taskAttrs.json` is this file:
```json
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

```json
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

## Handling intermediate workflow outputs

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


## Toplevel calls compiled as stages

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
