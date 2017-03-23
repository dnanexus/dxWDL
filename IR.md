# Intermediate Representation (IR)

The compiler is modularly split into two components, front-end and
back-end. The front-end takes a WDL workflow, and generates a
blueprint for a dnanexus workflow (*dx:workflow*). It works locally,
without making platform calls, and without using dnanexus data
structures. The blueprint has the format:

- List of *applet* definitions
- Serial list of stages, each using an applet

The back-end takes a blueprint, generates a dx:applet from each applet definition, and then
generates a dx:workflow that uses the applets in its stages.

The blueprint can be written to a file in human readable syntax,
privisionally YAML. The serialized form is intended for diagnostics,
automatic testing, and debugging.

## Applet definition

- name: applet name
- dx input: list of platform input arguments
- dx output: list of platform output arguments
- instace type: a platform instance name (optional)
- docker: docker image name (optional)
- destination : folder path on the platform
- language: computer language in which the script is written, could be
bash or WDL.
- entrypoint: starting point of execution in the code. For WDL, this
could be a scatter. For bash, normally `main`.
- code: bash or WDL snippet to exeute

## Workflow definition
List of stages, where a stage has the following fields:

- name: stage name
- applet: one of the pre-defined applets to execute
- inputs: list of arguments. They could be specified in several
different ways:
  * Constant
  * Name of an output/input from a previous stage
  * Empty. The user may specify a value at run time, alternatively,
    it may be an optional applet input.
