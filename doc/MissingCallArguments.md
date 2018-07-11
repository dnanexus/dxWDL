# Missing arguments

A workflow with missing call arguments is legal in WDL. For example,
workflow `trivial` calls task `add` with parameter `a` where
parameters `a` and `b` are required. If the user supplies
`b` at runtime, the workflow will execute correctly, otherwise, a
*missing argument* error will result.

```wdl
workflow trivial {
    input {
      Int x
    }
    call add {
      input: a=x
    }
    output {
        Int sum = add.result
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
```

The JSON inputs file below specifies `b`:
```
{
  "trivial.x" : 1,
  "trivial.add.b" : 3
}
```

In effect, missing call arguments are inputs to the workflow. This
also works for optional arguments. For example, if `add` has optional
argument `c`, then, it is possible to set it from a file:
```
{
  "trivial.x" : 1,
  "trivial.add.b" : 3,
  "trivial.add.c" : 10
}
```

This means that workflow `trivial` actually has four inputs: `{x,
add.a, add.b, add.c}`. A large realistic workflow makes many calls,
and has many hidden arguments. To implement this with a dnanexus
workflow, we need to materialize all these inputs, they cannot remain
hidden. The resulting platform workflow could easily have tens or
hundreds of inputs, making the user interface interface ungainly.

## Implementation issues

An implementation will need to start by adding the missing arguments. Let's
take a look at adding `trivial.add.b`. The workflow is rewritten to:

```wdl
workflow trivial {
    input {
      Int x
      Int add_b
    }
    call add {
      input: a=x, b=add_b
    }
    output {
        Int sum = add.result
    }
}
```

When reading the input file, we need to translate `trivial.add.b` to `trivial.add_b`. This requires
a mapping from workflow input arguments to their original names. This is extra
metadata for the compilation process; it is not simply just additional WDL code.

| new name | original |
| -------- | --------   |
| trivial.x | trivial.x  |
| trivial.add_b | trivial.add.b |

If a variable named `add_b` already exists, a new name is required for `add.b`.
Each workflow can go through multiple rewrite steps, each of which may encounter
naming collisions. For a complex workflow, the end result could be so different from
the original, as to be unrecognizable. Because names are mangled, following what
happens are runtime in the UI will be hard.


## Future direction: toplevel_calls_as_stages

There is an import use case where leaving task arguments unbound is
desirable. In the `detect_virus` workflow below, the
[scaffold](https://github.com/broadinstitute/viral-ngs/blob/master/pipes/WDL/workflows/tasks/assembly.wdl)
task takes 11 arguments. Users want to be able to open the UI and see
the `aligner` argument associated with the `scaffold` stage; further,
they want to be able to set it from that stage. With the CLI, they
would like to be able to do: `dx run detect_virus
-iscaffold.aligner="great_new_tool"`. Propagating `aligner` to a
workflow level input would make `detect_virus` less readable, while
also obscuring the utility of the argument.


```wdl
workflow detect_virus {
  File contigs_fasta

  call scaffold { input: contigs_fasta = contigs_fasta }

  ...
}

task scaffold {
  File         contigs_fasta
  File         reads_bam
  Array[File]+ reference_genome_fasta

  String? aligner
  Float?  min_length_fraction
  Float?  min_unambig
  Int?    replace_length=55

  Int?    nucmer_max_gap
  Int?    nucmer_min_match
  Int?    nucmer_min_cluster
  Int?    scaffold_min_pct_contig_aligned

  command {}
  output {
    ...
  }
}
```

The proposed `toplevel_calls_as_stages` flag will instruct dxWDL to
compile `detect_virus` to an unlocked dx:workflow with a stage for the
scaffold call. More generally, any toplevel call with no
subexpressions will be compiled to a stage. For example, in workflow
`foo`, only call `C` fits the bill.

```wdl

workflow foo {
  String who

  if (flag) {
     call A
  }

  scatter (i in [1,2,3]) {
    call B
  }

  call C { input: i=1 }

  call D { input: x= who + "__x"  }
}

task C {
   Int i
   output {
     Int result = i
   }
}


task D {
  String x
  String? y
  output {
     String result = x
  }
}
```
