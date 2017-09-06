# Release Notes

## 0.42
- WDL objects: experimental
- Type coercion
- Faster compilation when applets already exist. Using bulk describe API, instead
  of looking up each applet separately.
- Adding checksum to workflow, recompile only if it has not changed

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
