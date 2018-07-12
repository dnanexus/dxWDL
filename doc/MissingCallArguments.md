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


## Toplevel calls compiled as stages

There is an import use case where leaving task arguments unbound is
desirable. The `detect_virus` workflow below, is a simplified version
of the real world worklfow
[assemble_denovo_with_deplete_and_isnv_calling](https://github.com/broadinstitute/viral-ngs/blob/master/pipes/WDL/workflows/assemble_denovo_with_deplete_and_isnv_calling.wdl).
The three tasks have a large number of inputs. If we propage them to
the workflow level, we will have an unreadable script, and potential argument name collisions.
Users would like to be able to do:
```sh
dx run detect_virus -ideplete_taxa.query_chunk_size=200
```

This makes it clear that `query_chunk_size` is an input for the
`deplete_taxa` call, and not to any other call.


```wdl
workflow detect_virus {
  File raw_reads_unmapped_bam

  call deplete_taxa {
    input:
      raw_reads_unmapped_bam = raw_reads_unmapped_bam,
      bmtaggerDbs = bmtaggerDbs,
      blastDbs = blastDbs,
      bwaDbs = bwaDbs
  }

  call filter_to_taxon {
    input:
      reads_unmapped_bam = deplete_taxa.cleaned_bam
  }

  call assembly.assemble {
   input:
     reads_unmapped_bam = filter_to_taxon.taxfilt_bam,
     trim_clip_db = trim_clip_db
  }

  ...
}

task deplete_taxa {
  File         raw_reads_unmapped_bam
  Array[File]? bmtaggerDbs  # .tar.gz, .tgz, .tar.bz2, .tar.lz4, .fasta, or .fasta.gz
  Array[File]? blastDbs  # .tar.gz, .tgz, .tar.bz2, .tar.lz4, .fasta, or .fasta.gz
  Array[File]? bwaDbs  # .tar.gz, .tgz, .tar.bz2, .tar.lz4, .fasta, or .fasta.gz
  Int?         query_chunk_size
  Boolean?     clear_tags = false
  String? tags_to_clear_space_separated = "XT X0 X1 XA AM SM BQ CT XN OC OP"

  ...
}

task filter_to_taxon {
  File reads_unmapped_bam
  File lastal_db_fasta
  String bam_basename = basename(basename(reads_unmapped_bam, ".bam"), ".cleaned")
  ...
}

task assemble {
  File    reads_unmapped_bam
  File    trim_clip_db
  Int?    trinity_n_reads=250000
  Int?    spades_n_reads=10000000
  String? assembler="trinity"  # trinity, spades, or trinity-spades
  String  cleaned_assembler = select_first([assembler, ""])
  String  sample_name = basename(basename(reads_unmapped_bam, ".bam"), ".taxfilt")
  ...
}

```

The proposed `toplevel_calls_as_stages` flag will instruct dxWDL to
compile `detect_virus` to an unlocked dx:workflow with a stage for the
`deplete_taxa`, `filter_to_taxon`, and `assemble` calls. More
generally, any toplevel call with no subexpressions will be compiled
to a stage. For example, in workflow `foo`, only call `C` fits the
bill.

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

Turning the flag on has a downside, it reduces the opportunitues to
batch expression evaluation. In workflow `bar`, by default, the
calculation of `sz` and the call to `allele_freq` would be performed
in one job. With the flag turned on, these cannot be combined, and we need three separate
jobs. In addition, missing call arguments do not trigger a compile time error.

```wdl
workflow bar {
   File config
   Float overhead_factor

   call preamble

   Int sz = round(size(config) * overhead_factor)
   call allele_freq { disk_space = sz }
}
```
