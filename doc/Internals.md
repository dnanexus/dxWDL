# Compiler internals

The compiler is split into three passes
- Preprocess: simplify the original WDL code
- FrontEnd: take simplified WDL, and generate IR
- Backend: start with IR and generate platform applets and workflow

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

## Preprocessor
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

## Front end
The front-end takes the simplified WDL workflow, and generates a
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


## Backend
The back-end takes a blueprint, generates a *dx:applet* from each applet definition, and then
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
- wdlCode: WDL snippet to exeute

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
these structures uses dot, and superficially looks like call member access.
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
