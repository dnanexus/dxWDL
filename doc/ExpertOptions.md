# Detailed options for the expert

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
```
$ java -jar dxWDL-xxx.jar compile /path/to/foo.wdl
```
This compiles ```foo.wdl``` to platform workflow ```foo``` in dx's
current project and folder. The generated workflow can then be run as
usual using `dx run`. For example, if the workflow takes string
argument ```X```, then: ``` dx run foo -i0.X="hello world" ```

Compilation can be controled with several parameters.

| Option   |  Description |
| ------   | ------------ |
| archive  | Archive older versions of applets and workflows |
| defaults | A file with default parameter settings. The syntax is Cromwell style. |
| destination | Set the output folder on the platform |
| force    | Overwrite existing applets/workflows if they have changed |
| inputs   | A cromwell style inputs file |
| locked   | Create a locked-down workflow (experimental) |
| sort     | Sort call graph, to avoid forward references, used for CWL |
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
```
java -jar dxWDL-0.44.jar compile test/files.wdl -inputs test/files_input.json
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
```
dx run files -f test/files_input.dx.json
```

The `-defaults` option is similar to `-inputs`. It takes a JSON file with key-value pairs,
and compiles them as defaults into the workflow. If the `files.wdl` worklow is compiled with
`-defaults` instead of `-inputs`
```
java -jar dxWDL-0.44.jar compile test/files.wdl -defaults test/files_input.json
```

It can be run without parameters, for an equivalent execution.
```
dx run files
```

## Extensions

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

Normally, a file used in a task is downloaded to the instance, and
then used locally (*locallized*). If the file only needs to be
examined once in sequential order, then this can be optimized by
streaming instead. The Unix `cat`, `wc`, and `head` commands are of this
nature. To specify that a file is to be streamed, mark it as such in
the `parameter_meta` section. For example:

```
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

```
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

```
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

```
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

```
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

The dx:workflow that is compiled from `math` can set this variable from
the command line as follows:
```
dx run math -iadd.b=5
```

Setting workflow variables from the command line works for all unnested calls.
If the call is inside a scatter or an if block, this is not supported.

## Calling existing applets

Sometimes, it is desirable to call an existing dx:applet from a WDL
workflow. For example, when porting a native workflow, we can leave
the applets as is, without rewriting them in WDL. The `dxni`
subcommand, short for *Dx Native Interface*, is dedicated to this use
case. It searchs a platform folder and generates a WDL wrapper task for each
applet. For example, the command:

```
java -jar dxWDL.jar dxni --folder /A/B/C --output dx_extern.wdl
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
```
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

```
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
```
runtime {
   docker: "dx://GenomeSequenceProject:/A/B/myOrgTools"
}

runtime {
   docker: "dx://project-xxxx:record-yyyy"
}
```

## Debugging an applet

If you build an applet on the platform with dxWDL, and want to inspect
it, use: ```dx get --omit-resources <applet path>```. This will
refrain from downloading the large resource files that go into the
applet.


# Design

The main idea is to compile a WDL workflow into an equivalent DNAnexus
workflow, enabling running it on the platform. The basic mapping is:

1. A WDL Workflow compiles to a dx workflow
2. A WDL Call compiles to a dx workflow stage, and an applet (including invocation of dx-docker when called for)
3. A scatter block is compiled into a workflow stage.


The type mapping for primitive and single dimensional arrays
is as follows:

| WDL type       |  dxapp.json type |
| -------------- |  --------------- |
| Boolean        |   boolean    |
| Int            |   int  |
| Float          |   float |
| String         |   string |
| File           |   file |
| Array[Boolean] |   array:boolean    |
| Array[Int]     |   array:int  |
| Array[Float]   |   array:float |
| Array[String]  |   array:string |
| Array[File]    |   array:file |
| Complex types  |   hash + array:file |

Ragged arrays of files (Array[Array[File]]), and other more complex
WDL types, are mapped to two fields: a flat array of files, and a
hash, which is a json serialized representation of the WDL value. The
flat file array informs the job manager about data objects that need to
be closed and cloned into the workspace.

### Scatter

A scatter block is compiled into a workflow stage. The inputs for it
are the closure required for running. The outputs are the union of all
outputs of all the calls. For example, take a look at an example WDL:

```
task inc {
  Int i

  command <<<
    python -c "print(${i} + 1)"
  >>>

  output {
    Int incremented = read_int(stdout())
  }
}

task sum {
  Array[Int] ints

  command <<<
    python -c "print(${sep="+" ints})"
  >>>

  output {
    Int sum = read_int(stdout())
  }
}

workflow wf {
  Array[Int] integers = [1,2,3,4,5]
  scatter (i in integers) {
    call inc {input: i=i}
  }
  call sum {input: ints = inc.incremented}
}
```

The dx-workflow has two stages: ```scatter_inc```, and ```sum```. Each
implemented as an applet. The dxapp.json for the scatter
applet looks like this:

```
{
  "name" : "wf.scatter_inc",
  "inputSpec": [
     {"name": "integers" "class": "array:int"}
  ],
  "outputSpec": [
     {"name": "inc.incremented", "class": "array:int"}
  ]
  "runSpec": {
    "file": "src/code.sh",
    "interpreter": "bash",
    "distribution": "Ubuntu",
    "release": "14.04"
  }
}
```

The dxapp.json of the ```sum``` applet is:

```
{
  "name" : "wf.sum",
  "inputSpec": [
     {"name": "inc.incremented", "class": "array:int"}
  ],
  "outputSpec": [
     {"name": "sum.sum", "class": "int"}
  ]
  "runSpec": {
    "file": "src/code.sh",
    "interpreter": "bash",
    "distribution": "Ubuntu",
    "release": "14.04"
  }
}
```

At runtime, the scatter block is executed by the DxAppletRunner. It
schedules a job for each of the vectorized calls, and places JBORs to
link them together. It then waits for all jobs to complete, and
collects the ouputs into arrays.

## Challenges

We wish to avoid creating a controlling applet that would run and manage
a WDL workflow. Such an applet might get killed due to temporary
resource shortage, causing an expensive workflow to fail. Further, it is desirable
to minimize the context that needs to be kept around for the
WDL workflow, because it limits job manager scalability.

### Data types

WDL supports complex and recursive data types, which do not have
native support. In order to maintain the usability of the UI, when possible,
we map WDL types to the dx equivalent. This works for primitive types
(Boolean, Int, String, Float, File), and for single dimensional arrays
of primitives. However, difficulties arise with complex types. For
example, a ragged array of strings `Array[Array[String]]` presents two issues:

1. Type: Which dx type to use, so that it will be presented intuitively in the UI
2. Size: variables of this type can be very large, we have seen 100KB
sized values. This is much too large for a dx:string, that is passed to
the bash, stored in a database, etc.

### Scatters

A scatter could include multiple calls, for example:

```
scatter (i in integers) {
  call inc {input: i=i}
  call mul {input: i=i}
  call factor {input: i=i}
}
```

It would have been easy to group the three straight line calls `{inc,
mul, factor}` into one workflow, and call it once for each collection
element. However, this is not supported natively. The compiler needs
to generate a launcher applet, to start a job for each `i`, while
taking care not to create a controlling applet running during the
entire scatter operation.

### Declarations

WDL allows placing a declaration anywhere in a workflow. Such
a statement could perform file access, or any calculation,
requiring a compute instance. For example:
```
workflow wf {
  File bam_table
  Array[Int] ia = [1,2,3,4,5]
  Int j = ia[1]
  Int k = j + 23
  Array[Array[String]] = read_tsv(bam_table)
```

This requires starting compute instances even for innocuous looking
declarations. This causes delays, incurrs expenses, and is surprising
to the user.
