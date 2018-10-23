# Changes between WDL draft-2 and version 1.0

First off, note that draft-3 and version 1.0 are one and the
same. This is a potential point of confusion.

The main improvements between WDL version 1.0 and draft-2 are:

1. Much improved syntax for the task
   [Command Section](https://github.com/openwdl/wdl/blob/master/versions/1.0/SPEC.md#command-section). This
   allows mixing shell code and WDL code, which is required in
   tasks. The challenge is that these are two different languages,
   with different grammars but sharing symbols.

2. Well defined inputs for
   [workflows](https://github.com/openwdl/wdl/blob/master/versions/1.0/SPEC.md#workflow-inputs)
   and
   [tasks](https://github.com/openwdl/wdl/blob/master/versions/1.0/SPEC.md#task-inputs). Previously,
   inputs and expressions were indistinguishable, and dxWDL required a
   heuristic to figure out when a variable was an input. The heuristic
   was sometimes wrong.

3. Addition of standard library methods, missing in draft-2. For example, flatten (https://github.com/openwdl/wdl/blob/master/versions/1.0/SPEC.md#arrayx-flattenarrayarrayx).

4. Structs
https://github.com/openwdl/wdl/blob/master/versions/1.0/SPEC.md#struct-definition
