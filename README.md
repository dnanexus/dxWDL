# dxWDL

dxWDL takes a bioinformatics pipeline written in
[Workflow Description Language (WDL)](http://www.openwdl.org/)
and compiles it to an equivalent workflow on the DNAnexus platform.


## Release status
<a href="https://travis-ci.org/dnanexus/dxWDL"><img src="https://travis-ci.org/dnanexus/dxWDL.svg?branch=master"/></a>

dxWDL provides a reasonably complete set of WDL features for beta
testing. A few significant WDL features are not yet supported,
and they are under development:

- Nested workflows (sub-workflows)
- Nested scatters, and conditionals nested in scatters

*Use at your own risk:* for the time being, dxWDL is an exploratory
 tool NOT covered by DNAnexus service and support agreements. We
 welcome feedback and provide assistance as time permits.

## Getting started
Prerequisites: DNAnexus platform account, dx-toolkit, java 8+, python 2.7.

Make sure you've installed the dx-toolkit CLI, and initialized it with
`dx login`. Download the latest compiler jar file from the
[releases](https://github.com/dnanexus/dxWDL/releases) page.


## Workflow

```wdl
workflow bam_chrom_counter {
    File bam

    call slice_bam {
        input :
               bam = bam
    }
    scatter (slice in slice_bam.slices) {
        call count_bam {
            input:
                    bam = slice
        }
    }
    output {
        slice_bam.bai
        count_bam.count
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


## Compiling
To compile a workflow:
```
$ java -jar dxWDL-xxx.jar compile /path/to/foo.wdl
```
This compiles ```foo.wdl``` to platform workflow ```foo``` in dx's
current project and folder. The generated workflow can then be run as
usual using `dx run`. For example, if the workflow takes string
argument ```X```, then: ``` dx run foo -i0.X="hello world" ```

Compilation can be controled with several parameters, the most
frequently used ones are described in the table below.

| Option   |  Description |
| ------   | ------------ |
| force    | Overwrite existing applets/workflows if they have changed |
| inputs   | A cromwell style inputs file |
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
