# Demo

## Hello_world

Compiling the hello_world WDL file, with the verbose flag provides more information on what
the compiler is doing.
```
$ java -jar dxWDL-0.32.jar compile hello_world -verbose
```

```
Preprocessing pass
simplifying scatters
simplifying workflow top level
Wrote simplified WDL to /tmp/dxWDL_Compile/hello_world.simplified.wdl
FrontEnd pass
Accessed tasks = Set(AddNumbers)
FrontEnd: compiling tasks into dx:applets
Compiling task AddNumbers
FrontEnd: compiling workflow
Compiling common applet hello_world_common
Wrote intermediate representation to /tmp/dxWDL_Compile/hello_world.ir.yaml
Backend pass
Compiling applet AddNumbers
Building applet AddNumbers
dx build /tmp/dxWDL_Compile/AddNumbers --destination project-xxxx:/AddNumbers
Applet AddNumbers = applet-yyyy
Compiling applet hello_world_common
Building applet hello_world_common
dx build /tmp/dxWDL_Compile/hello_world_common --destination project-xxxx:/hello_world_common
Applet hello_world_common = applet-zzzz
Stage hello_world_common = stage-uuuu
Stage AddNumbers = stage-vvvv
workflow-xxxx
```

The result is a DNAx workflow, that can be executed from the command line with `dx`.
```
$ dx run hello_world -ia=1 -ib=2
```

## GATK best practices pipeline
Compile the workflow, and provide an input file.
```
$ java -jar dxWDL-0.32.jar compile test/gatk_170412.wdl -inputs test/gatk_170412_exome_input.json
```

Run the workflow
```
$ dx run /builds/2017-06-28/PairedEndSingleSampleWorkflow -f test/gatk_170412_exome_input.dx.json
```
