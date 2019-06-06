# Release Notes

## 1.09
**Fixed**
- The `-p` flag was not respected


## 1.08
**Fixed**
- [bug 259](ttps://github.com/dnanexus/dxWDL/issues/259). Unified the code for resolving platform paths.
- Error when downloading a file that resides in a container, rather than a project.
- Imports specified from the command line

**Changed**
- Merged `DxPath` and `DxBulkResolve` modules.

## 1.06
- Upgrade to Cromwell v41
- Mark auxiliary workflows and applets as hidden. This is a step in the direction of supporting copying of a workflow from one project to another.
- Added unit tests for the `WomValueAnalysis` module, which checks if WOM expressions are constant.
- Reducing reliance on the dxjava library, calling the DXAPI directly.
- Merged fork of the dxjava library back into the dx-toolkit.
- Use the dx-download-agent (dxda) instead of `dx download` in tasks.
- Fix for bug (https://github.com/dnanexus/dxWDL/issues/254)
- Fix for bug occurring when a `struct` is imported twice

## 1.05
- Got DxNI to work for DNAnexus apps, not just applets.
- Added links to applets that are referenced inside WDL fragments. This should allow, at some point, copying workflows between projects.
- Moved applet meta-information into the `details` field. This makes dx:applets more readable.
- Wrote unit-tests for the `WdlVarLinks` module.
- Batching file-describe, and file-resolve calls.

## 1.04
- Precalculate instance types. There are tasks that calculate the instance type they need in the runtime section. If the task call is made from a workflow fragment, we can calculate the instance type then and there. This allows launching directly in the correct instance type, instead of launching an additional job.
- Fixed bug in expression evaluation at runtime (https://github.com/dnanexus/dxWDL/issues/240)
- Improved unit-test coverage

## 1.03
- Bug fixes related to optional arguments (https://github.com/dnanexus/dxWDL/issues/235).

## 1.02
- Removed the instance-type database, and wom source code from the inputs of applets.
- Added the WDL source code to workflow and applet objects on the platform. It is stored in the details field, and
can be easily [retrieved](./doc/ExpertOptions.md#Getting-WDL-sources). It has been removed from the generated applet bash script.
- Initial support for the `struct` type
- Check that the reserved substring '___' is not used in the source WDL code. This sequence is used
  to translate dots ('.') into DNAx inputs and outputs. Dots are invalid symbols there.
- Bug fixes: https://github.com/dnanexus/dxWDL/issues/224, https://github.com/dnanexus/dxWDL/issues/227, https://github.com/dnanexus/dxWDL/issues/228

## 1.01
- Ensure that native docker uses the machine's hostname (i.e., the job ID) as
  the hostname, matching the previous behavior of dx-docker. This allows
  setting the job ID in error messages, helping debugging. Contributed by Jeff Tratner.

## 1.00
- Support for WDL version 1.0, as well as draft-2. There are two features
that are not yet supported: `struct`, and nested scatters. Work is ongoing
to address these omissions.

## 0.81.5
- Adding debug information to conversions from JSON to WDL variables. This helps
track down runtime problems with missing variables that a task is expecting. Previously,
we didn't know which variable was having a problem.

## 0.81.4
- Bug fix for complex cases where WDL files import each other.

## 0.81.3
- Bug fix for native docker. There was a problem when a docker image was using an internally defined
user; it didn't have the necessary permissions to create output files on the worker.

## 0.81.2
- Support NTLM proxies. If your organization is configured with an NTLM proxy,
you can use it like this:
```
$ export HTTP_PROXY_METHOD=ntlm
$ export HTTP_PROXY_DOMAIN = acme.com
$ export HTTP_PROXY = https://john_smith:welcome1@proxy.acme.com:8080
$ java -jar dxWDL.jar ...
```


## 0.81.1
- Supporting proxy configuration with user and password. For example:
```
$ export HTTPS_PROXY = https://john_smith:welcome1@proxy.acme.com:8080
```

## 0.81

- [Proxy configuration](./doc/ExpertOptions.md#Proxy-configurations). If your organization interposes a proxy between the internal machines and external hosts, you can set the environment variable `HTTP_PROXY` (or `HTTPS_PROXY`) to point to the proxy. The compiler will pass all of its dnanexus API calls through that proxy. For example, if you perform the following on the command line shell:

```bash
$ export HTTP_PROXY = proxy.acme.com:8080
$ java -jar dxWDL.jar ...
```

the compiler will route all requests through the machine `proxy.acme.com` on port `8080`.

## 0.80

- Native docker is now the default. If you still want to use [dx-docker](https://wiki.dnanexus.com/Developer-Tutorials/Using-Docker-Images), the `-useDxDocker` flag is available. In order to store a docker image on the platform, you can do `docker save`, and upload the tarball to a file. More details are provided in the [export options](./doc/ExpertOptions.md#Docker).

- The compiler emits a warning for partialy defined workflow outputs. For example, in workflow `foo`, the output `add.result` is partial, because it is not assigned to a variable. Partial definitions are discarded during compilation, hence the warning.

```wdl
workflow foo {
  call add { ... }
  output {
     add.result
  }
}
```

To avoid this problem, rewrite like this:

```wdl
workflow foo {
  call add { ... }
  output {
     Int r = add.result
  }
}
```

- Update test scripts for python3

## 0.79.1

- Version [docker-based runner script](https://github.com/dnanexus/dxWDL/tree/master/scripts/compiler_image/run-dxwdl-docker)
- Do not call sudo in runner script, in case system is set up not to require
  sudo to run Docker.
- Rename run script to `run-dxwdl-docker`

```
$ export DXWDL_VERSION=0.79.1
$ sudo run-dxwdl-docker compile /path/to/foo.wdl -project project-xxx
```

## 0.79
- Support [per task dx-attributes](./doc/ExpertOptions.md#Setting-dnanexus-specific-attributes-for-tasks).
- Report a warning for a non-empty runtime section in a native applet, instead of throwing an error. Note
that the WDL runtime section will be ignored, the native definitions will be used instead.
- Fix bug when using [spaces in output files](https://github.com/dnanexus/dxWDL/issues/181)
- Eliminate a job-describe API call from all tasks. This reduces overall platform load,
which is important in volume workflows.
- Support for [private docker registries](./doc/ExperOptions.md#Native-docker-and-private-registries)
- A [docker image](https://hub.docker.com/r/dnanexus/dxwdl) for
running the compiler without needing to install dependencies. You can
use the
[dxwdl](https://github.com/dnanexus/dxWDL/tree/master/scripts/compiler_image/dxwdl)
script to compile file `foo.wdl` like this:

```bash
$ runc.sh compile /path/to/foo.wdl -project project-xxxx
```

- Instructions for how to replace the built in
  [reorganize workflow outputs](./doc/ExpertOptions.md#Use-your-own-applet) applet,
  with your [own](./doc/ExpertOptions.md#Handling-intermediate-workflow-outputs).


## 0.78.1
- Support the `restartableEntryPoints` applet option in the `extras` file.

## 0.78
- Clone the dxWDL runtime asset to local project, to allow sub-jobs access to it.

## 0.77
- Improve user message when pretty printing an erroneous WDL file.
- New command line flag `--leaveWorkflowsOpen`, that leaves the toplevel workflow
open. This option is intended for power users, it allows modifying the workflow
after the compiler is done.
- Figure out the smallest instance from the *current* price list,
avoid the use of a hardcoded instance type. On the Azure cloud, an
applet failed because it tried using the hardcoded `mem1_ssd1_x4`
instance, which does not exist there (only on AWS).

## 0.76
- Handle using an asset that lives in another project by creating a local
record for it.

## 0.75
- Upgrade to Ubuntu 16.04
- Preparatory work for supporting WOM as a compilation intermediate representation.

## 0.74.1
- Removed inlining of tasks code into auxiliary applets. This was causing
a workflow to change when a task was modified, violating separate compilation,
and not allowing executable cloning to work.

## 0.74
- Improving test and release scripts.
- Adding printout for the dxWDL version to running applets and workflows
- Check and throw an exception if an asset is not in the current project. It
needs to be cloned.
- Supporting Amsterdam (azure:westeurope) and Berlin (aws:eu-central-1) regions.
- Fixed error in collecting results from an optional workflow branch.

## 0.72
- Put the project-wide reuse of applets under a special flag `projectWideReuse`.
- Improved the queries to find dx:executables on the target path.
- Improvements to the algorithm for splitting a WDL code block into parts.

## 0.71
- In an unlocked workflow, compile toplevel calls with no
subexpressions to dx stages. The
[expert options](./doc/ExpertOptions.md#toplevel-calls-compiled-as-stages)
page has a full description of the feature.
- Allow the compiler to reuse applets that have been archived. Such
applets are moved to a `.Archive` directory, and the creation date is
appended to the applet name, thereby modifying the applet name. The
name changes causes the search to fail. This was fixed by loosening the
search criterion.

## 0.70
- Upgrade to Cromwell 33.1
- Reuse applets and workflows inside a rpoject. The compiler now looks
for an applet/workflow with the correct name and checksum anywhere in
the project, not just in the target directory. This resolved
issue (https://github.com/dnanexus/dxWDL/issues/154).

## 0.69
- Support importing http URLs
- Simplified handling of imports and workflow decomposition
- Fixed issue (https://github.com/dnanexus/dxWDL/issues/148). This was a bug
occuring when three or more files with the same name were downloaded to
a task.
- Fixed issue (https://github.com/dnanexus/dxWDL/issues/146). This
occurred when (1) a workflow called a task, and (2) the task and the workflow had
an input with the same name but different types. For example, workflow
`w` calls task `PTAsays`, both use input `fruit`, but it has types
`Array[String]` and `String`.

```wdl
workflow w {
    Array[String] fruit = ["Banana", "Apple"]
    scatter (index in indices) {
        call PTAsays {
            input: fruit = fruit[index], y = " is good to eat"
        }
        call Add { input:  a = 2, b = 4 }
    }
}

task PTAsays {
    String fruit
    String y
     ...
}
```

## 0.68.1
- Fixing build and release scripts.
- Allowing non-priviliged users to find the dxWDL runtime asset in
the public repository.

## 0.68
- Throw an exception if a wrapper for a native platform call has
a non-empty runtime section.
- Use an SSD instance for the collect sub-jobs.
- Remove the runtime check for calling a task with missing values,
fix issue (https://github.com/dnanexus/dxWDL/issues/112). The check
is overly restrictive. The task could have a default, or, be able to
tolerate the missing argument.

## 0.67
- Color coding outputs, yellow for warnings, red for errors.
- Indenting information output in verbose mode
- Removing the use of `dx pwd`. The user needs to specify the
destination path on the command line. A way to avoid this, is to compile
from the dx-toolkit, with the upcoming `dx compile` command.


## 0.66.2
- Allow variable and task names to include the sub-string "last"
- Workflow fragment applets inherit access properties from the extras
file.

## 0.66.1
- Ignoring unknown runtime attributes in the extras file.

## 0.66
- Decomposing a workflow when there is a declaration after a call. For example,
workflow `foo` needs to be decomposed. The workflow fragment runner does not
handle dependencies, and cannot wait for the `add` call to complete.

```wdl
workflow foo {
    Array[Int] numbers

    scatter (i in numbers) {
        call add { input: a=i, b=1}
        Int m = add.result + 2
    }

    output {
        Array[Int] ms = m
    }
}
```

- Setting debug levels at runtime. The compiler flag `runtimeDebugLevel` can be set to 0, 1, or 2.
Level 2 is maximum verbosity, level 1 is the default, zero is minimal outputs.

- Upgrade to Cromwell version 32.

- Using headers to speed up the workflow decomposition step. The idea
is to represent a WDL file with header. When a file is imported, we
use the header, instead of pulling in the entire WDL code, including
its own imports.

- Support setting defaults in applets, not just workflows. This can be done with the `--defaults`
command line option, and a JSON file of WDL inputs.

## 0.65
- Optimization for the case of launching an instance where it is
calculated at runtime.

- Support dnanexus configuration options for tasks. Setting
the execution policy, timeout policies, and access
control can be achieved by specifying the default option in the
`default_taskdx_attributes` section of the `extras` file. For
example:

```json
{
  "default_task_dx_attributes" : {
    "runSpec": {
        "executionPolicy": {
          "restartOn": {
            "*": 3
          }
        },
        "timeoutPolicy": {
          "*": {
            "hours": 12
          }
        },
        "access" : {
          "project": "CONTRIBUTE",
          "allProjects": "VIEW",
          "network": [
            "*"
          ],
          "developer": true
        }
      }
  }
}
```

- Improved error message for namespace validation. Details are no longer hidden when
the `-quiet` flag is set.

- Reduced logging verbosity at runtime. Disabled printing of directory structure when running
tasks, as the directories could be very large.

- Added support for calling native DNAx apps. The command

```
java -jar dxWDL.jar dxni -apps -o my_apps.wdl
```

instructs the compiler to search for all the apps you can call, and create WDL
tasks for them.


## 0.64
- Support for setting defaults for all task runtime attributes has been added.
This is similar to the Cromwell style. The `extras` command line flag takes a JSON file
as an argument. For example, if `taskAttrs.json` is this file:
```json
{
    "default_runtime_attributes" : {
      "docker" : "quay.io/encode-dcc/atac-seq-pipeline:v1"
    }
}
```

Then adding it to the compilation command line will add the `atac-seq` docker image to all
tasks by default.
```
java -jar dxWDL-0.44.jar compile test/files.wdl -defaults test/files_input.json -extras taskAttrs.json
```

- Reduced the number of auxiliary jobs launched when a task specifies the instance type dynamically.
A task can do this is by specifiying runtime attributes with expressions.
- Added the value iterated on in scatters

## 0.63
- Upgrade to cromwell-31 WDL/WOM library
- Report multiple validation errors in one step; do not throw an exception for
the first one and stop.
- Reuse more of Cromwell's stdlib implementation
- Close generated dx workflows


## 0.62.2
- Bug fix for case where optional optional types were generated. For example, `Int??`.

## 0.62

- Nested scatters and if blocks
- Support for missing arguments has been removed, the compiler will generate
an error in such cases.


## 0.61.1

- When a WDL workflow has an empty outputs section, no outputs will be generated.

## 0.61

- Decomposing a large block into a sub-workflow, and a call. For
example, workflow `foobar` has a complex scatter block, one that has
more than one call. It is broken down into the top-level workflow
(`foobar`), and a subworkflow (`foobar_add`) that encapsulates the
inner block.

```
workflow foobar {
  Array[Int] ax

  scatter (x in ax) {
    Int y = x + 4
    call add { input: a=y, b=x }
    Int base = add.result
    call mul { input: a=base, n=x}
  }
  output {
    Array[Int] result = mul.result
  }
}

task add {
  Int a
  Int b
  command {}
  output {
    Int result = a + b
  }
}

task mul {
  Int a
  Int n
  command {}
  output {
    Int result = a ** b
  }
}
```

Two pieces are together equivalent to the original workflow.
```
workflow foobar {
  Array[Int] ax
  scatter (x in ax) {
    Int y = x + 4
    call foobar_add { x=x, y=y }
  }
  output {
    Array[Int] result = foobar_add.mul_result
  }
}

workflow foobar_add {
  Int x
  Int y

  call add { input: a=y, b=x }
  Int base = add.result
  call mul { input: a=base, n=x}

  output {
     Int add_result = add.result
     Int out_base = base
     Int mul_result = mul.result
   }
 }
```

- A current limitation is that subworkflows, created in this way, give errors
for missing variables.
- The allowed syntax for workflow outputs has been tightened. An output
declaration must have a type and and a value. For example, this is legal:
```
output {
   Int add_result = add.result
}
```

but this is not:
```
output {
   add.result
}
```

## 0.60.2
- Minor bug fixes

## 0.60.1
- Bug fix release

## 0.60
- Split README into introduction, and advanced options. This should, hopefully, make
the top level text easier for the beginner.
- A workflow can call another workflow (subworkflow). Currently,
  the UI support for this feature is undergoing improvements.
- The search path for import can be extended by using the `--imports` command line argument.
This is useful when the source WDL files are spread across several directories.

## 0.59
- Improved handling of pricing list
- Upgraded to the new wdl4s library, that now lives in the cromwell
  repository under cromwell-wdl. The cromwell version is 30.2.

## 0.58
- Adhering to WDL spec: a declaration set to a constant is
treated as an input. In the example, `contamination` is compiled to
a workflow input with a default of `0.75`.

```
workflow w {
  Float contamination = 0.75
}
```

- Improving naming of workflow stages, and how their appear in the UI.
- Fixed regression in compilation of stand-alone applets.
- Fixed bug when pretty-printing applets that use <<<,>>> instead of {,}

## 0.57
- Fixed bug in setting workflow defaults from JSON file
- Improved behavior of the `sub` and `size` stdlib functions
- Improved naming convention for scatter/if workflow stages.

## 0.56
- Access to arguments in calls inside scatters. This
feature was lost while supporting locked-down workflows.

```
task Add {
    Int a
    Int b
    command {}
    output {
        Int result = a + b
    }
}

workflow w {
    scatter (i in [1, 10, 100]) {
        call Add { input: a=i }
    }
}
```

For example, in workflow `w`, argument `b` is missing from the `Add`
call. It is now possible to set it it from the command line with `dx
run w -iscatter_1.Add_b=3`. With an inputs file, the syntax is:
`w.Add.b = 3`.

- Fixed bad interaction with the UI, where default values were omitted.

## 0.55
- Improved support for conditionals and optional values
- Automated tests for both locked and regular workflows
- Various minor bug fixes

## 0.54
- More accurate detection of the IO classes in dx:applets.
- Improved handling of WDL constants. For example, multi word
constants such as `["1", "2", "4"]`, and `4 + 11` are recognized as
such. When possible, they are evaluated at runtime.
- Revert default compilation to a regular workflow. It is possible to
compile to a locked-down workflow by specifying `-locked` on the
command line. To specify command workflow inputs from the command line
use:
```
dx run foo -i0.N=19
```


## 0.53
- Not closing dx:workflow objects, this is too restrictive.


## 0.52
- Uplift the code to use DNAnexus workflow inputs and outputs.
Running a workflow now has slighly easier command line syntax.
For example, if workflow `foo` takes an integer
argument `N`, then running it through the CLI is done like
this:

```
dx run foo -iN=19
```

- Revamp the conversions between dx:file and wdl:file. This allows
specifying dx files in defaults.
- Initial support for non-empty WDL array types, for example `Array[String]+`.
- Improved handling of optional values

- An experimental new syntax for specifying inputs where the workflow
is underspecified. WDL allows leaving required call inputs unassigned, and
specifying them from the input file. For example, workflow `math`
calls task `add`, but does not specify argument `b`. It can then
be specified from the input file as follows: `{ "math.add.b" : 3}`.

```
task add {
    Int a
    Int b
    output {
        Int result = a + b
    }
}

workflow math {
    call add { input: a = 3 }
    output {
       add.result
    }
}
```

The dx:workflow that is compiled from `math` can set this variable from
the command line as follows:
```
dx run math -iadd___b=5
```

This command line syntax may change in the future.

## 0.51
- Adding a `-quiet` flag to suppress warning and informational
  messages during compilation.
- Minor fixes

## 0.50
- Tasks that have an empty command section do not download files eagerly. For example,
if a task only looks at `size(file)`, it will not download the file at all.

```
task fileSize {
    File in_file

    command {}
    output {
        Float num_bytes = size(in_file)
    }
}
```

- Support docker images that reside on the platform.


## 0.49
- Removed limitation on maximal hourly price per instance. Some bioinformatics pipelines
regularly require heavy weight instances.
- Fixed several WDL typing issues
- Improving the internal representation of JSON objects given as input.

## 0.48
- Support string interpolation in workflow expressions
- Handle scatters where the called tasks return file arrays, and non-native
platform types. This is done by spawning a subjob to collect all the outputs
and bundle them.
- Azure us-west region is supported

## 0.47
- Support calling native DNAx applets from a WDL workflow. A helper utility
  is `dxni` (*Dx Native Interface*), it creates task wrappers for existing
  dx:applets.

## 0.46
- Allow applet/workflow inputs that are optional, and have a default.
- More friendly command line interface

## 0.45
- Default workflow inputs. The `--defaults` command line argument
embeds key-value pairs as workflow defaults. They can be overridden
at runtime if necessary.

## 0.44
- Use hashes instead of files for non-native dx types
- Do not use the help field in applet input/output arguments to carry
  WDL typing information.
- Fold small text files into the applet bash script. This speeds up the 'dx build'
  command, and avoids uploading them as separate platform files.
- Replace 'dx build' with direct API calls, additional compilation speedup.

## 0.43
- Speed up the creation of a dx:workflow. With one API call create
all the workflow stages, and add set the checksum property.
- Speeding up applet construction

## 0.42
- WDL objects: experimental
- Type coercion
- Faster compilation when applets already exist. Using bulk describe API, instead
  of looking up each applet separately.
- Adding checksum to dx:workflow objects, recompile only if it has not changed
- Specified behavior of input/output files in doc/Internals.md

## 0.41
- Minor fixes to streaming

## 0.40
- Upgrade to wdl4s version 0.15.
- Enable several compiler warnings, removing unused imports
- Bug fixes:
  * Wrong dx project name when using multiple shells, and
    different projects
  * Better handling of errors from background 'dx cat' processes.

## 0.39
- Streaming works with tasks that use docker

## 0.38
- Minor fixes, and better testing, for file streaming

## 0.37
- File download streaming

## 0.36
- Conditionals

## 0.35
- Support white space in destination argument
- Allow providing the dx instance type directly in the runtime attributes.

## 0.34
- Topological sorting of workflow. By default, check for circular
dependencies, and abort in case there are cycles. Optionally, sort the
calls to avoid forward references. This is useful for WDL scripts
that were not written by hand.
- Create an `output_section` applet for a compiled workflow. If
`--reorg` is specified on the command line, the applet also
reorganizes the output folder; it moves all intermediate results to
subfolder `intermediate`. Reorganization requires `CONTRIBUTE` level access,
which the user needs to have.
- Multi region support.

## 0.33
- Initial support for WDL pairs and maps
- Upgrade to wdl4s version 0.13
- Improve parsing for memory specifications
- Optionals can be passed as inputs and outputs from tasks.

## 0.32
- Support archive and force flags a la `dx build`. Applets and workflows
  are *not* deleted by default, the --force flag must be provided.
- If there are insufficient permissions to get the instance price list, we
  have a reasonable fallback option.
- Added namespace concept to intermediate representation.
- WDL pretty printer now retains output section.

## 0.31
- The compiler is packed into a single jar file
- Fixed glob bug

## 0.30
Bug fix release.

Corrected the checksum algorithm, to recurse through the directory
tree.

## 0.29
Improved support for scatters
  * Support an expression in a scatter collection.
  * Compile declarations before a scatter into the scatter
  applet. This optimization folds two applets into one.
  * Use wdl4s native name resolution to calculate scatter
  collection type.
  * Added documentation to doc/IR.md describing how scatters
  are compiled.

## 0.28
- Support for ragged file arrays
- Correctly handle an empty workflow output section
- Upgrade to wdl4s version 0.12
- Improvements to regression tests

## 0.27
- Support the import directive. This allows spreading definitions
  across multiple files.
- Using native wdl4s data structures, instead of pretty printing, for
  internal representation.

## 0.26
- Support passing unbound variables to calls inside scatters
- Implemented glob and size function for files
- Correctly handling empty arrays in task input/output
- Improved scatter linking. Got rid of runtime calls to get dx:applet descriptions.

## 0.25
- Allow a docker image to be specified as a parameter
- Use Travis containers for continuous integration
- Adding tests for instance type choice at runtime

## 0.24
- Support resource requirements (memory/disk) calculated from runtime variables.

## 0.23
- Rebuild an applet, only if the source code has changed. For example,
if an applet's WDL code changed, it will be rebuilt in the next compilation.

## 0.22
- Files are lazily downloaded in auxiliary applets, such as scatters. In
  tasks/applets, they are always downloaded.

## 0.21
- Adding linking information to applets that call other applets. This
ensures the correct applet-ids are invoked. No lookup by name
is performed at runtime.
- Minor bug fixes

## 0.20
- Separate compilation of workflows and tasks. A task is compiled to
single dx:applet, and a workflow is compiled to auxiliary applets and
a dx:workflow.
- WDL files containing only tasks, and no workflow, are supported
- Many improvements all around

## 0.13
- Upgrade to wdl4s v0.10
- Improving version number handling
- Minor improvements in debugging printout and error reporting

## 0.12
- The GATK pipeline compiles and runs (result not validated yet)
- Bug fixes:
  * Correct order of scatter output arrays
  * Improved handling of optionals

## 0.11
- Better compilation error messages
- Version checking
- Compiler verbose mode
- Renamed initial workflow stage to Common
