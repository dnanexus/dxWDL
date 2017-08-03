# dxWDL

dxWDL takes a bioinformatics pipeline written in
[Workflow Definition Language](https://software.broadinstitute.org/wdl)
and compiles it to an equivalent workflow on the DNAnexus platform.


## Release status
<a href="https://travis-ci.org/dnanexus-rnd/dxWDL"><img src="https://travis-ci.org/dnanexus-rnd/dxWDL.svg?branch=master"/></a>

dxWDL provides a reasonably complete set of WDL features for beta
testing, in particular enabling the [GATK best practices pipeline](https://github.com/broadinstitute/wdl/blob/develop/scripts/broad_pipelines/PublicPairedSingleSampleWf_170412.wdl).
A few significant WDL features are not yet supported, but may be
added according to user interest:

- Nested workflows (sub-workflows)
- Nested scatters
- Objects


*Use at your own risk:* for the time being, dxWDL is an exploratory
 tool NOT covered by DNAnexus service and support agreements. We
 welcome feedback and provide assistance on a best-effort basis.

## Getting started
Prerequisites: DNAnexus platform account, dx-toolkit, java 8+, python 2.7.

Make sure you've installed the dx-toolkit CLI, and initialized it with
`dx login`. Download the latest compiler jar file from the
[releases](https://github.com/dnanexus-rnd/dxWDL/releases) page.

To compile a workflow:
```
$ java -jar dxWDL-xxx.jar compile /path/to/foo.wdl
```
This compiles ```foo.wdl``` to platform workflow ```foo``` in dx's
current project and folder. The generated workflow can then be run as
usual using `dx run`. For example, if the workflow takes string
argument ```X```, then: ``` dx run foo -i0.X="hello world" ```

Compilation can be controled with several parameters.

| Option  |  Description |
| ------  | ------------ |
| archive | Archive older versions of applets (*dx build -a*)|
| destination | Set the output folder on the platform |
| force   | Delete existing applets/workflows |
| sort    | Sort call graph, to avoid forward references, used for CWL |
| verbose | Print detailed progress information |


## Extensions (experimental)

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

## Debugging an applet

If you build an applet on the platform with dxWDL, and want to
inspect it, use: ```dx get --omit-resources  <applet path>```. This will refrain from
downloading the large resource files that go into the applet.


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
| Complex types  |   file + array:file |

Ragged arrays of files (Array[Array[File]]), and other more complex
WDL types, are mapped to two fields: a flat array of files, and a
file, which is a json serialized representation of the WDL value. The
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
