# Compiler internals

The compiler processes a WDL file in several phases:

- WOM: use the Cromwell
[WOM library](https://github.com/broadinstitute/cromwell/tree/develop/wom/src)
to read a WDL file, parse it, type check it, and create a WOM data
structure represention.
- IR: generate Intermediate Code (_IR_) from the WOM representation
- Native: start with IR and generate platform applets and workflow

The main idea is to compile a WDL workflow into an equivalent DNAnexus
workflow, enabling running it on the platform. The basic mapping is:

* A WDL task compiles to a DNAx applet (_dx:applet_).
* A WDL workflow compiles to a DNAx workflow (_dx:workflow_)
* A WDL call compiles to a dx workflow stage, and sometimes an auxiliary applet
* Scatters and conditional blocks are compiled into workflow stages.

There are multiple obstacles to overcome. We wish to avoid creating a
controlling applet that would run and manage a WDL workflow. Such an
applet might get killed due to temporary resource shortage, causing an
expensive workflow to fail. Further, it is desirable to minimize the
context that needs to be kept around for the WDL workflow, because it
limits job manager scalability.


## Type mapping
WDL supports complex and recursive data types, which do not have
native support. In order to maintain the usability of the UI, when possible,
we map WDL types to the dx equivalent. This works for primitive types
(`Boolean`, `Int`, `String`, `Float`, `File`), and for single dimensional arrays
of primitives. However, difficulties arise with complex types. For
example, a ragged array of strings `Array[Array[String]]` presents two issues:

1. Type: Which dx type to use, so that it will be presented intuitively in the UI
2. Size: variables of this type can be very large, we have seen 100KB
sized values. This is much too large for a dx:string, that is passed to
the bash, stored in a database, etc.

The type mapping for primitive types is:

| WDL type       |  dxapp.json type |
| -------------- |  --------------- |
| Boolean        |   boolean |
| Int            |   int  |
| Float          |   float |
| String         |   string |
| File           |   file |


Optional primitives are mapped as follows:

| WDL type       |  dxapp.json type | dxapp.json optional |
| -------------- |  --------------- | -------------       |
| Boolean?       |   boolean    | true |
| Int?           |   int        | true |
| Float?         |   float      | true |
| String?        |   string     | true |
| File?          |   file       | true |


Single dimensional arrays of WDL primitives are mapped to DNAx optional arrays, because
it allows them to be empty. Had they been mapped to compulsory arrays, they
would be required to have one element at least.

| WDL type       |  dxapp.json type | dxapp.json optional |
| -------------- |  --------------- | -------------       |
| Array[Boolean] |   array:boolean    | true |
| Array[Int]     |   array:int  | true |
| Array[Float]   |   array:float | true |
| Array[String]  |   array:string | true |
| Array[File]    |   array:file | true |


WDL types that fall outside these categories (e.g. ragged array of
files `Array[Array[File]]`) are mapped to two fields: a flat array of
files, and a hash, which is a json serialized representation of the
WDL value. The flat file array informs the job manager about data
objects that need to be closed and cloned into the workspace.

## Imports and nested namespaces

A WDL file creates its own namespace. It can also import other files,
each inhabiting its own sub-namespaces. Tasks and workflows from
children can be called with their fully-qualified-names. We map the
WDL namespace hierarchy to a flat space of *dx:applets* and
*dx:workflows* in the target project and folder. To do this, we
make sure that tasks and workflows are uniquely named.

In a complex namespace, a task/workflow can have several definitions. Such
namespaces cannot be compiled by dxWDL.


## Compiling a task

A task is compiled into an applet that has an equivalent
signature. For example, a task such as:

```wdl
task count_bam {
    input {
        File bam
    }
    command <<<
        samtools view -c ${bam}
    >>>
    runtime {
        docker: "quay.io/ucsc_cgl/samtools"
    }
    output {
        Int count = read_int(stdout())
    }
}
```

is compiled into an applet with the following `dxapp.json`:

```json
{
  "name": "count_bam",
  "dxapi": "1.0.0",
  "version": "0.0.1",
  "inputSpec": [
    {
      "name": "bam",
      "class": "file"
    }
  ],
  "outputSpec": [
    {
      "name": "count",
      "class": "int"
    }
  ],
  "runSpec": {
    "interpreter": "bash",
    "file": "code.sh",
    "distribution": "Ubuntu",
    "release": "16.04"
  }
}
```

The `code.sh` bash script runs the docker image `quay.io/ucsc_cgl/samtools`. Under
that image, it run the shell command `samtools view -c ${bam}`.

## Compiling workflows

This section examines sample workflows and shows how they are
compiled. We start from simple workflows, and work our way to complex
workflows. For simplicity, the values and types are WDL
integers. Files, maps, arrays, and other types could be substituted,
without changing the core concepts. The following tasks are called
throughout:

```wdl
# Add two integers
task add {
  input {
    Int a
    Int b
  }
  command {}
  output {
    Int result = a + b
  }
}

# Multiply two integers
task mul {
  input {
    Int a
    Int b
  }
  command {}
  output {
    Int result = a * b
  }
}

# Reduce add one to an integer
task inc {
  input {
    Int a
  }
  command {}
  output {
    Int result = a + 1
  }
}
```

### A linear workflow

Examine workflow `linear` below:

```wdl
workflow linear {
  input {
    Int x
    Int y
  }

  call add {input: a = x, b = y }
  call mul {input: a = add.result, b = 2 }
  call int {input: a = mul.result }

  output {
    Int result = sub.result
  }
}
```

It has no expressions, and no if/scatter blocks. It can be compiled directly to a dx:workflow,
which schematically looks like this:


| phase   | call   | arguments |
|-------  | -----  | ----      |
| Inputs  |        |     x, y  |
| Stage 1 | applet add | x, y  |
| Stage 2 | applet mul | stage-1.result, 2 |
| Stage 3 | applet inc | stage-2.result |
| Outputs |        | sub.result |

### Expressions

Workflow `linear2` adds expressions.

```wdl
workflow linear2 {
    input {
        Int x
        Int y
    }

    call add {
        input: a=x, b=y
    }

    Int z = add.result + 1
    call mul { input: a=z, b=5 }

    call inc { input: i= z + mul.result + 8}

    output {
        Int result = inc.result
    }
}
```

DNAnexus workflows do not support expressions, therefore, we need to generate an applet,
and run a job to evaluate each expression. Workflow `linear2` is compiled into:

| phase   | call   | arguments |
|-------  | -----  | ----      |
| Inputs  |        |     x, y  |
| Stage 1 | applet add | x, y  |
| Stage 2 | applet fragment_mul | stage-1.result |
| Stage 3 | applet fragment_inc | stage-2.result |
| Outputs |        | sub.result |

The `fragment_mul` applet performs this WDL snippet:

```wdl
    Int z = add.result + 1
    call mul { input: a=z, b=5 }
```

It returns the variables `z` and `mul.result`.

The `fragment_inc` applet performs

```wdl
call inc { input: i= z + mul.result + 8}
```

It evaluates the expression, calls the `inc` applet, and returns a promise for the output `inc.result`.
