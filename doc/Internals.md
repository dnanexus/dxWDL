# Compiler internals

The compiler is split into three passes
- Simplify: simplify the original WDL code
- IR: take simplified WDL, and generate Intermediate Code (IR)
- Native: start with IR and generate platform applets and workflow

To explain these, we will walk through the compilation process of a simple WDL file.

```
task Add {
    Int a
    Int b
    command {
        echo $((a + b))
    }
    output {
        Int result = a + b
    }
}

workflow math {
    Int i
    Int k

    call Add {
         input: a = i*2, b = k+4
    }
    call Add as Add2 {
         input: a = Add.result + 10, b = (k*2) + 5
    }
    output {
        Add2.result
    }
}
```

## Simplification step
The preprocessor starts with the original WDL, and simplifies it,
writing out a new WDL source file. A call that has subexpressions is
rewritten into separate declarations and a call with variables and
constants only. Declarations are collected to reduce the number of
jobs needed for evaluation.

The output of the preprocessor for the `math` workflow is:
```
workflow math {
    Int i
    Int k
    Int xtmp1 = k+4
    Int xtmp2 = i*2
    Int xtmp3 = (k*2) + 5

    call Add {
         input: a = xtmp2, b = xtmp1
    }

    Int xtmp4 = Add.result + 10
    call Add as Add2 {
         input: a = xtmp4, b = xtmp3
    }

    output {
        Add2.result
    }
}
```

In this case, the top four declarations will be calculated with one
job, and xtmp3 will require an additional job. This pass allows
mapping calls to platform workflow stages, which do not support subexpressions.

## Generating intermediate representation
The IR stage takes the simplified WDL workflow, and generates a
blueprint for a dnanexus workflows and applets (*dx:workflow*, *dx:applet*). It works locally,
without making platform calls, and without using dnanexus data
structures. The blueprint has the format:
- List of *dx:applet* definitions
- Serial list of stages, each using an applet

For the `math` workflow, we get the following abbreviated intermediate code:
```yaml
name: math
stages:
- name: Common
  applet: math_common
  inputs: i, k
  outputs: Int i, Int k, Int xtmp1, Int xtmp2, Int xtmp3
- name: Add
  applet: Add
  inputs: Common:i, Common:k
  outputs: Int result
- name: Eval1
  applet: Eval1
  inputs: Add:result
  outputs: Int xtmp4
- name: Add2
  applet: Add
  inputs: Eval1:xtmp4, Common:xtmp3
  outputs: Int result

applets:
- name: Add
  inputs:
    - Int a
    - Int b
  outputs:
    - Int result
  wdlCode: |
task Add {
    Int a
    Int b
    command {
        echo $((a + b))
    }
    output {
        Int result = a + b
    }
}

- name: Common
  inputs:
    - Int i
    - Int k
  outputs:
    - Int i
    - Int k
    - Int xtmp1
    - Int xtmp2
    - Int xtmp3
  wdlCode: |
workflow w {
    Int i
    Int k
    output {
        Int xtmp1 = k+4
        Int xtmp2 = i*2
        Int xtmp3 = (k*2) + 5
    }
}

- name: Eval1
  inputs:
     - Int x
  outputs:
     - Int result
  wdlCode: |
task Eval1 {
    Int x
    command {
    }
    output {
        Int result = x + 10
    }
}
```


## Native
The backend takes a blueprint, generates a *dx:applet* from each applet definition, and then
generates a *dx:workflow* that uses the applets in its stages.

The blueprint can be written to a file in human readable syntax,
provisionally YAML. The serialized form is intended for diagnostics,
automatic testing, and debugging.

## IR definition
### IR Applet

- name: applet name
- inputs: list of WDL input arguments
- outputs: list of WDL output arguments
- instace type: a platform instance name
- docker: docker image name (optional)
- destination : folder path on the platform
- ns: WDL namespace to execute, an applet or a workflow

### IR Workflow
List of stages, where a stage has the following fields:

- name: stage name
- applet: one of the pre-defined applets to execute
- inputs: list of arguments. These could be either empty, constants,
  or point to an output from a previous stage.
- outputs: argument names and types


## Scatters

A scatter loops over a collection, which may be a complex
expression. To implement scatters, we use an applet that launches
sub-jobs, and leaves links to their results. Examine the workflow
snippet below.

```
scatter (k in [1,2,3]) {
    call lib.Inc as inc3 {input: i=k}
}
```

Variable ```k``` is looping over an integer array. The Preprocessor
extracts the collection expression into a separate declaration.
```
Array[Int] xtmp0 = [1,2,3]
scatter (k in xtmp0) {
    call lib.Inc as inc3 {input: i=k}
}
```

The FrontEnd packs the declaration and scatter into one applet,
so we do not spawn a separate job to calculate `xtmp0`.

Workflow `sg_sum3` presents a more complex case. It imports
tasks from the math library, and takes a `numbers` input array.

```
import "library_math.wdl" as lib

workflow sg_sum3 {
    Array[Int] numbers

    scatter (k in range(length(numbers))) {
        call lib.Inc as inc {input: i= numbers[k]}
        call lib.Mod7 as mod7 {input: i=inc.result}
    }
    Array[Int] partial = inc.results
    scatter (k in mod7.result) {
        call lib.Inc as inc2 {input: i=k}
    }
}
```

It is simplified into:
```
import "library_math.wdl" as lib

workflow sg_sum3 {
    Array[Int] numbers
    Array[Int] xtmp0 = range(length(numbers))
    scatter (k in xtmp0) {
        call lib.Inc as inc {input: i= numbers[k]}
        call lib.Mod7 as mod7 {input: i=inc.result}
    }
    Array[Int] partial = inc.results
    Array[Int] xtmp1 = mod7.result
    scatter (k in xtmp1) {
        call lib.Inc as inc2 {input: i=k}
    }
}
```

Three applets are generated, `common`, `scatter_1`, and `scatter_2`.

#### common
```
    Array[Int] numbers
    Array[Int] xtmp0 = range(length(numbers))
```
Accept workflow input arguments (`numbers`), and calculate the
`range(length(numbers))` expression.

#### scatter_1
```
    scatter (k in xtmp0) {
        call lib.Inc as inc {input: i= numbers[k]}
        call lib.Mod7 as mod7 {input: i=inc.result}
    }
```
Loop over the `xtmp0` array.

#### scatter_2
```
    Array[Int] partial = inc.results
    Array[Int] xtmp1 = mod7.result
    scatter (k in xtmp1) {
        call lib.Inc as inc2 {input: i=k}
   }
```
Execute two declarations and a scatter. This avoids
creating a fourth applet to calculate the `partial` and `xtmp1`
arrays.

## Member accesses vs. Call member access

WDL supports tuples and objects. The syntax for accessing members in
these structures uses dot, and syntactically is the same is call member access.
For example, in workflow `salad`, the `pair.left` syntax is similar to
calling task `pair` and accessing member `left`.

```
workflow salad {
    Map[String, Int] weight = {"apple": 100, "banana": 150}

    scatter(pair in weight) {
        String name = pair.left
    }
}
```


In workflow `chef` we want to call a task with the two members
of the pair `sign`. This cannot be compiled directly into a dx:stage, because
the arguments {`a`,`b`} need to be extracted first; the job manager does
not know how to access members in a JSON hash.

```
task Concat {
    String a
    String b
    command {
        echo "${a}${b}"
    }
    output {
        String result = stdout()
    }
}

workflow chef {
    Pair[String, String] sign = ("chef", "Julian Dremond")
    call concat{ input: a=sign.left, b=sign.right }
```

The workflow is rewritten into `chef_simplified`, with temporary variables
for the pair members. The downside is that the caculating the new variables may, in
certain cases, require an additional sub-job.

```
workflow chef_simplified {
    Pair[String, String] sign = ("chef", "Julian Dremond")
    String xtmp1 = sign.left
    String xtmp2 = sign.right
    call concat{ input: a=xtmp1, b=xtmp2 }
```

## Workflow outputs

A workflow declares its outputs via an output section, which may define
new variable names, access call results, and evaluate expressions. For example,
see workflows `salad`, and `salad2`.

```
task fruit {
    output {
        Array[String] ingredients
        Int num_veggies
    }
}

workflow salad {
    call fruit { input: }

    output {
       fruit.ingredients
       fruit.num_veggies
     }
}

workflow salad2 {
    call fruit { input: }

    output {
       Int veg_to_buy = fruit.num_veggies * 2
    }
}
```

The output section is compiled into an applet called in the last stage
of the *dx:workflow*. The applet has to evaluate the output expressions,
correctly pass in the required closure, and rename variables when necesseray.
For example, `fruit.ingredients` is not a valid dx output parameter,
and it must be renamed. However, renaming can cause name collisions. An additional
difficultly is that WDL does not allow an output variable to have the same
name as an input variable.

To handle these difficulties, we add an "out_" prefix to outputs,
and replace dots with underscores. For example:

```
workflow salad_output {
   Array[String] fruit_ingredients
   Int fruit_num_veggies

   output {
       Array[String] out_fruit_ingredients = fruit_ingredients
       Int out_fruit_num_veggies = fruit_num_veggies
   }
}

workflow salad2_output {
   Int fruit_num_veggies

   output {
       Int out_veg_to_buy = fruit_num_veggies * 2
   }
}
```


A workflow may create hundreds of result files, only a small subset of
which are declared in the output section. The applet organizes the
output directory structure, and moves all non-final result files into
subdirectory `intermediate`. This requires `CONTRIBUTE` applet
*dx:permissions*, and is optional.


## Multiple Regions

The platform runs on several cloud regions, each of which is
independent. Files cannot be cloned between regions, which presents a
difficulty, because the dxWDL runtime library is such a file. In fact,
it is a `dx:asset`, not a simple file.

The compiler jar file includes a configuration file, with a list of
regions, and their assets. When compiling in project *P*, in region
*R*, compiled applets and workflows will be stored in *P*, and
reference assets residing only in region *R*. When creating a new
dxWDL release, the runtime library is copied to all supported regions,
and an updated configuration file folded into the dxWDL jar file.


## File paths

When WDL code executes on a platform instance, it assumes the runtime
system places input files in a known location. This is called *file
localization*, and is a bit underspecified in the existing WDL
[draft specification](https://github.com/broadinstitute/wdl/blob/develop/SPEC.md). This
section describes the dxWDL implementation.

A task can have input files, for example, task `wc` takes `in_file`.

```
task wc {
    File in_file

    command {
        cat ${in_file} | wc -l
    }
    output {
        Int count = read_int(stdout())
    }
}
```

The task runs under the dnanexus user, whose home directory is
`DX_HOME`. All input WdlFiles are downloaded from the platform to the
directory `DX_HOME/execution/inputs/`. For example, if file `Foo.txt` is
an input, a read-only copy is placed in
`DX_HOME/execution/inputs/Foo.txt`. If an additional `Foo.txt` file is an
input, and it's file ID is `file-xxxx`, it will be downloaded to
`DX_HOME/execution/inputs/file-xxxx/Foo.txt`.

An output WDL file is specified by its local path, and will be uploaded to
a platform destination retaining its name. The rest of the path is
ignored. For example, WdlFile `DX_HOME/X/Y/myOutput.tsv` is
uploaded to a platform file named `myOutput.tsv`. The output path can
be set in the *dx:workflow*. If there are several output WdlFiles with
the same name, they will be versions of the same platform file. For
example, `DX_HOME/Z/myOutput.tsv` will be a such second
version.

The issues with this implementation are:
1. There is no way to set the platform path for an individual WdlFile
2. There is a mismatch between the platform, that allows file versioning,
   and WdlFiles, that do not.

In addition, some WDL constructs allow examining the path of a
WdlFile. This information then leaks to downstream workflow stages,
making the execution dependent on the runtime system implementation.


## Calling dx:applets from a WDL workflow

Sometimes, it is useful to be able to call existing DNAx applets. For example,
an existing *dx:workflow* can be imported into WDL this way, without having
to rewrite all the called applets. To achieve this, a compilation step called
*Foreign Function Interface* (FFI) is implemented.

FFI takes a platform path (project:folder) where the applets can be
found. It generates a WDL header for each dx:applet, and writes
these headers into a file. The WDL workflow imports this file, and can
subsequentally call these applets.

For example, applet `test/applets/mk_int_list` takes two integers, and
returns a list with both. The python code is:

```python
def main(**job_inputs):
    a = job_inputs['a']
    b = job_inputs['b']

    # Return output
    output = {}
    output["all"] = [a, b]
    return output

dxpy.run()
```

The input/output declaration in the `dxapp.json` is:
```json
  "inputSpec": [
    {
      "name": "a",
      "class": "int"
    },
    {
      "name": "b",
      "class": "int"
    }
  ],
  "outputSpec": [
    {
      "name": "all",
      "class": "array:int"
      }
  ]
```

The FFI pass creates a WDL header that looks like this:
```
task mk_int_list {
  Int a
  Int b
  command {}
  output {
    Array[Int] all = []
  }
  meta {
    type: extern
    applet_id: applet-xxxx
  }
}
```

In the same spirit as C external declarations, this is an empty task
used to compile the WDL workflow. At runtime, the applet-id
specified in the `meta` section is called.
