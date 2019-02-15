<a href="https://travis-ci.org/dnanexus/dxWDL"><img src="https://travis-ci.org/dnanexus/dxWDL.svg?branch=master"/></a>

dxWDL takes a bioinformatics pipeline written in the
[Workflow Description Language (WDL)](http://www.openwdl.org/)
and compiles it to an equivalent workflow on the DNAnexus platform.
It provides a reasonably complete set of WDL features.
WDL draft-2 is supported, with a few exceptions:
* Calls with missing arguments have limited support
* The ouput section:
1. does not support expressions
2. requires full definitions. For example:
```wdl
workflow foo {
  output {
     String result = bar
  }
}
```

Instead of
```wdl
workflow foo {
  output {
     bar
  }
}
```

# Ongoing
Work has started on supporting WDL version 1.0 using the WOM scala library,
that is part of [Cromwell](https://github.com/broadinstitute/cromwell). A high level
list of changes between draft-2 and version 1.0 is provided [here](doc/WdlVersionChanges.md).
We plan to remove the limitations on the output section.

## Setup
Prerequisites: DNAnexus platform account, dx-toolkit, java 8+, python 2.7.

Make sure you've installed the dx-toolkit CLI, and initialized it with
`dx login`. Download the latest compiler jar file from the
[releases](https://github.com/dnanexus/dxWDL/releases) page.


## Example workflow

The `bam_chrom_counter` workflow is written in WDL. Task
`slice_bam` splits a bam file into an array of sub-files. Task
`count_bam` counts the number of alignments on a bam file. The
workflow takes an input bam file, calls `slice_bam` to split it into chromosomes, and
calls `count_bam` in parallel on each chromosome. The results comprise a
bam index file, and an array with the number of reads per chromosome.

```wdl
workflow bam_chrom_counter {
    File bam

    call slice_bam {
        input : bam = bam
    }
    scatter (slice in slice_bam.slices) {
        call count_bam {
            input: bam = slice
        }
    }
    output {
        File bai = slice_bam.bai
        Int count = count_bam.count
    }
}

task slice_bam {
    File bam
    Int num_chrom = 22
    command <<<
    set -ex
    samtools index ${bam}
    mkdir slices/
    for i in `seq ${num_chrom}`; do
        samtools view -b ${bam} -o slices/$i.bam $i
    done
    >>>
    runtime {
        docker: "quay.io/ucsc_cgl/samtools"
    }
    output {
        File bai = "${bam}.bai"
        Array[File] slices = glob("slices/*.bam")
    }
}

task count_bam {
    File bam
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

From the command line, we can compile the workflow to the DNAnexus platform using the dxWDL jar file.
```
$ java -jar dxWDL-0.59.jar compile bam_chrom_counter.wdl -project project-xxxx
```

This compiles the source WDL file to several platform objects.
- A workflow `bam_chrom_counter`
- Two applets that can be called independently: `slice_bam`, and `count_bam`
- A few auxiliary applets that process workflow inputs, outputs, and launch the scatter.

These objects are all created in the current `dx` project and folder. The generated workflow can
be run using `dx run`. For example:
```
dx run bam_chrom_counter -i0.file=file-xxxx
```

At runtime this looks like this:
![this](doc/bam_chrom_counter.png)

## Compiler docker image

If you wish to avoid installing java and the dxWDL jar file, you can use
the public docker image `dnanexus/dxwdl`, placed on dockerhub. The
[run-dxwdl-docker](scripts/compiler_image/run-dxwdl-docker)
script makes it easier to use. For example:

```bash
$ source environment
$ dx login
$ run-dxwdl-docker compile xxxx.wdl --project MY_PROJECT
```

# Additional information

- [Advanced options](doc/ExpertOptions.md) explains additional compiler options
- [Internals](doc/Internals.md) goes into compiler internals
- [Tips and tricks](doc/Tips.md) commonly used patterns in WDL

# Contributions

This software is a community effort! You can browse any of the contributions below in
our [contrib](contrib) folder, or [let us know](https://github.com/dnanexus/dxWDL/issues)
if you would like to contribute or request a feature.

 - [docker](contrib/docker): A Dockerfile to deploy the software ([@vsoch](https://www.github.com/vsoch))
