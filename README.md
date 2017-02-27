# dxWDL

dxWDL takes a bioinformatics pipeline written in [Workflow Definition Language](https://github.com/broadinstitute/wdl) and "compiles" it to an equivalent workflow on the DNAnexus platform.

To learn more on WDL, please check out these links:
* [tutorial WDLs](https://github.com/broadinstitute/wdl/tree/develop/scripts/tutorials/wdl)
* [GATK production WDLs](https://github.com/broadinstitute/wdl/tree/develop/scripts/broad_pipelines)
* [WDL spec](https://github.com/broadinstitute/wdl/blob/develop/SPEC.md)
* [wdl4s scaladoc](http://broadinstitute.github.io/wdl4s/0.9/#package) - the compiler uses this library to parse WDL input

## Release status

This project is in alpha testing, and does not yet support all WDL features. Initially, we have focused on enabling the [GATK best
practices pipeline](https://github.com/broadinstitute/wdl/blob/develop/scripts/broad_pipelines/PublicPairedSingleSampleWf_160927.wdl). Other
features will be added as the project matures and according to user interest.

The main WDL features not yet supported are:
- Nested workflows (sub-workflows)
- Definitions spread across multiple files (import)
- Conditionals
- Non-constant declarations in the middle of a workflow or scatter
- Resource requirements (memory/disk) calculated from runtime variables

## Getting started

Prerequisites: DNAnexus platform account, dx-toolkit, java 8+, python 2.7.

Make sure you've installed the dx-toolkit CLI, and initialized it with
`dx login`. Fetch the dxWDL wrapper script to a local directory:

```
$ mkdir dxWDL
$ cd dxWDL
$ dx download dxWDL:/dxWDL_latest -fo dxWDL
$ chmod +x dxWDL
```

The wrapper script will automatically download a few JAR dependencies to the working directory the first time it runs. To compile a workflow:
```
$ ./dxWDL compile /path/to/foo.wdl
```
This compiles ```foo.wdl``` to platform workflow ```foo``` in dx's current project and folder. The generated workflow can then be run as usual using `dx run`. For example, if the workflow takes string argument
```X```, then:
```
dx run foo -i0.X="hello world"
```

## Usage tips

If you build an applet on the platform with dxWDL, and want to
inspect it, use: ```dx get --omit-resources  <applet path>```. This will refrain from
downloading the large resource files that go into the applet.

# Developer zone

This section is intended for people interested in making modifications
to the compiler, or building it on their own.

## Installation of software prerequisits

Download the Broad Institute tools, get the latest versions of
cromwell and wdl4s. Currently, we are working with wdl4s version
[0.9](http://broadinstitute.github.io/wdl4s/0.9/#package), and
cromwell version
[24](https://github.com/broadinstitute/cromwell/releases/tag/24).

The instructions here assume an Ubuntu 16.04 system (Xenial).

Install java v1.8
```
sudo apt install openjdk-8-jre-headless
```

Install Scala
```
wget www.scala-lang.org/files/archive/scala-2.11.8.deb
sudo dpkg -i scala-2.11.8.deb
```

Get ```sbt```, this is a make like utility that works with the ```scala``` language.
```
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
sudo apt-get update
sudo apt-get install sbt
```

Running sbt for the first time takes several minutes, because it
downloads all required packages.


Checkout the code, and build it.
```
git clone git@github.com:dnanexus/dx-toolkit.git
git clone git@github.com:dnanexus-rnd/dxWDL.git
make -C dx-toolkit java
mkdir dxWDL/lib && cp dx-toolkit/lib/java/dnanexus-api-0.1.0-SNAPSHOT-jar-with-dependencies.jar dxWDL/lib
cd dxWDL && make all
```

The dxWDL/lib subdirectory contains the java bindings for dnanexus,
the dxjava jar file. This allows the compilation process to find dx
methods, without including them in the final (fat) jar file. In order
to compile a WDL file into a dx-workflow, do:

```
make asset
```

This will create an asset that includes all the applet dependencies in the current
dx project. Record the asset id, `record-xxxx`.

Then:
```
./dxWDL compile yourWorkflow.wdl --asset record-xxxx
```

## SBT tips

### cache

sbt keeps the cache of downloaded jar files in
```${HOME}/.ivy2/cache```. For example, the WDL jar files are under
```${HOME}/.ivy2/cache/org.broadinstitute```. In case of problems with
cached jars, you can remove this directory recursively. This will make
WDL download all dependencies (again).

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
| Array[Array[*]] |   file |

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
