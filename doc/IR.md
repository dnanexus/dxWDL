# Intermediate Representation (IR)

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
