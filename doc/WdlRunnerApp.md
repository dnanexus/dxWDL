# WDL Runner App

This page discusses a possible resesign of the WDL runner, to make it
app based.

Today, we build a custom applet to run each workflow fragment. Instead, it may be preferable
to have a single app that can run any fragment. The app will have a signature like this:
- input: hash containing WDL inputs
- output: hash containing WDL outputs
- files: an array of dx:files
- sourceCode: WDL script

Using hashes to wrap around inputs and outputs is defined as *boxed*
calling convention. Using native dnanexus types (*int*, *float*,
*dx:file*, ...) to accept inputs and return outputs is defined as
*unboxed*.

A workflow is decomposed into fragments, each fragment uses boxed
calling convention. A task is into a custom applet to allow setting
the runspec, access, and various other attributes. It uses boxed
calling convention, so that calling tasks will not require unboxing.
Calling native executables (dx:app, dx:applet, dx:workflow) requires
unboxing, while collecting the results requires boxing.

Examine workflow `foo`.

```
version 1.0

workflow foo {
  input {
    Int n
    Int x
  }
  scatter (i in range(n)) {
    call add {input: a = i, b= x }
  }
  if (n > 3) {
    call sum {input: numbers = add.result}
  }

  output {
    Array[Int] results = add.result
    Int total? = sum.result
  }
}

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

task sum {
  input {
    Array[Int] numbers
  }
  command { ... }
  output {
    Int result = read_int(stdout())
  }
}
```

Each task will be compiled into a custom dx:applet. The signatures
will look like this:

```
dx:applet add
- input: hash
- output: hash
- files: file:array
- code: ...
```

The workflow will compile into a locked dx:workflow with two stages:

```
stage-0
  source code: "scatter (i in range(n))"
  call wdl_runner_app:
     code =
        scatter (i in range(n)) {
           call add {input: a = i, b= x }
       }

stage-1
  source code: "if (n > 3)"
  call wdl_runner_app:
     code =
        if (n > 3) {
           call sum {input: numbers = add.result}
        }
```
